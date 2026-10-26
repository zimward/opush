use std::{pin::Pin, str::FromStr, time::Duration};

use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{net::TcpStream, select};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{error, info, trace, warn};

use crate::client::{
    Url,
    protocol::{ClientMessage, ServerMessage},
};

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsSource = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

#[derive(Debug)]
pub enum ConError {
    // Connection loss etc
    Network,
    //reconnecting doesn't make sense. IE. TLS errors, server not found etc
    NetworkCritical,
    Permission,
    Protocol,
}

impl From<tokio_tungstenite::tungstenite::Error> for ConError {
    // TODO: check for more possibly critical errors
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        trace!("tungstenite error: {:#?}", value);
        match value {
            tokio_tungstenite::tungstenite::Error::ConnectionClosed
            | tokio_tungstenite::tungstenite::Error::AlreadyClosed => Self::Network,
            tokio_tungstenite::tungstenite::Error::Io(_error) => Self::Network,
            tokio_tungstenite::tungstenite::Error::Tls(_tls_error) => Self::NetworkCritical,
            tokio_tungstenite::tungstenite::Error::Capacity(_capacity_error) => Self::Protocol,
            //treat http protocol errors as critical connection closing errors
            tokio_tungstenite::tungstenite::Error::Protocol(_protocol_error) => Self::Network,
            tokio_tungstenite::tungstenite::Error::WriteBufferFull(_message) => Self::Protocol,
            tokio_tungstenite::tungstenite::Error::Utf8(_)
            | tokio_tungstenite::tungstenite::Error::AttackAttempt => Self::Protocol,
            tokio_tungstenite::tungstenite::Error::Url(_url_error) => Self::Protocol,
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                if response.status() == 401 {
                    Self::Permission
                } else {
                    Self::Network
                }
            }
            tokio_tungstenite::tungstenite::Error::HttpFormat(_error) => Self::Protocol,
        }
    }
}

#[derive(Debug)]
enum PingState {
    // hello has not yet arrived, statemachine must first be reset
    Stop,
    // waiting for either sending the next application ping or receiving a server ping
    Waiting {
        // use future here?
        next_ping: Pin<Box<tokio::time::Sleep>>,
    },
    // waiting for server pong after sending ping
    WaitingForPong {
        //point in time where the pong must have arrived
        timeout: Pin<Box<tokio::time::Sleep>>,
    },
}

impl PingState {
    pub const fn new() -> Self {
        Self::Stop
    }
    pub fn start(ping_interval: Duration) -> Self {
        Self::Waiting {
            next_ping: Box::pin(tokio::time::sleep(ping_interval)),
        }
    }
    fn wait_for_pong(timeout: Duration) -> Self {
        Self::WaitingForPong {
            // use a hardcoded timeout of 5s for now
            timeout: Box::pin(tokio::time::sleep(timeout)),
        }
    }
    /// if a ping arrived next should be called with `server_ping = true`, otherwise we just check the wait
    /// returns `Err()` variant on a timeout. Future Will never Yield if the statemachine is stopped.
    async fn next(
        &mut self,
        server_ping: bool,
        ping_interval: Duration,
        pong_timeout: Duration,
    ) -> Result<Option<ClientMessage>, ()> {
        match self {
            Self::Stop => {
                // statemachine is stopped, so make sure next never advances as the stream's future must be selected
                // to get the Hello event
                futures_util::future::pending::<()>().await;
                Err(()) //unreachable
            }
            Self::Waiting { next_ping } => {
                if server_ping {
                    // return pong, reset client ping timer as we just got ping'ed
                    info!("sending client pong!");
                    *self = Self::start(ping_interval);
                } else {
                    next_ping.await;
                    info!("sending client ping!");
                    //send client ping
                    *self = Self::wait_for_pong(pong_timeout);
                }
                Ok(Some(ClientMessage::Ping))
            }
            Self::WaitingForPong { timeout } => {
                if server_ping {
                    // pong arrived. only non-error transition where we don't want to ping
                    info!("received server pong!");
                    *self = Self::start(ping_interval);
                    Ok(None)
                } else {
                    timeout.await;
                    warn!("didn't receive server pong!");
                    //timeout didn't get canceled in time. connection needs to be restarted
                    *self = Self::Stop;
                    Err(())
                }
            }
        }
    }
}

#[derive(Debug)]
struct PingFSM {
    ping_interval: Duration,
    pong_timeout: Duration,
    state: PingState,
}

impl PingFSM {
    pub const fn new(ping_interval: Duration, pong_timeout: Duration) -> Self {
        Self {
            ping_interval,
            pong_timeout,
            state: PingState::new(),
        }
    }

    /// Call when Hello message has been received to start timers
    pub fn start(&mut self) {
        self.state = PingState::start(self.ping_interval);
    }
    /// Call with `server_ping=true` if a ping message arrived, in which case this function will always yield immediately
    ///
    pub async fn next(&mut self, server_ping: bool) -> Result<Option<ClientMessage>, ()> {
        let res = self
            .state
            .next(server_ping, self.ping_interval, self.pong_timeout)
            .await;
        trace!("PingFSM state: {:?}", self.state);
        res
    }

    /// Resets the statemachine into its Stopped state for when the connection broke
    pub fn reset(&mut self) {
        self.state = PingState::new();
    }

    pub const fn is_started(&self) -> bool {
        !matches!(self.state, PingState::Stop)
    }
}

#[derive(Debug)]
pub struct WsCon {
    url: Url,
    sink: Option<WsSink>,
    source: Option<WsSource>,
    ping_fsm: PingFSM,
}
impl WsCon {
    pub const fn new(url: Url) -> Self {
        Self {
            url,
            sink: None,
            source: None,
            ping_fsm: PingFSM::new(Duration::from_secs(125), Duration::from_secs(5)),
        }
    }

    pub async fn connect(&mut self, hello: ClientMessage) -> Result<(), ConError> {
        if self.is_connected() {
            warn!("Attempted to open a already opened connection!");
            return Ok(());
        }
        if let ClientMessage::Hello { uaid } = hello {
            //no need to check if connection has been lost here as we never established one
            let (ws, _) = connect_async(&self.url).await?;
            let (sink, source) = ws.split();
            self.sink = Some(sink);
            self.source = Some(source);
            self.send_msg(ClientMessage::Hello { uaid }).await
        } else {
            unreachable!("called connect with a message that isn't client hello.");
        }
    }
    /// Filters out pings to drive the application `PingFSM` and checks if a Hello has been received yet
    async fn inspect_message(
        &mut self,
        msg: Result<ServerMessage, ConError>,
    ) -> Result<Option<ServerMessage>, ConError> {
        match msg {
            Ok(msg) => {
                match msg {
                    ServerMessage::Ping => {
                        if self.ping_fsm.is_started() {
                            // no need to handle the Err(()) variant as it can't be returned if a ping arrived,
                            // the FSM will either be in Waiting and transition to Pong or be in WaitingForPong and
                            // transition to Waiting
                            if let Ok(o) = self.ping_fsm.next(true).await
                                && let Some(msg) = o
                            {
                                // send ping event here as we can't pass a mut borrow of self into the fsm
                                // due to the select! in `receive`
                                let _ = self.send_msg(msg).await;
                            }
                        } else {
                            error!(
                                "Received Server Ping before FSM was started. This is a violation of the protocol and therefore a BUG!"
                            );
                        }
                        Ok(None)
                    }
                    ServerMessage::Hello {
                        uaid,
                        status,
                        use_webpush,
                    } => {
                        trace!("Hello received. Starting PingFSM");
                        // connection established. start application ping fsm
                        self.ping_fsm.start();
                        //forward Hello to client message loop
                        Ok(Some(ServerMessage::Hello {
                            uaid,
                            status,
                            use_webpush,
                        }))
                    }
                    msg => Ok(Some(msg)),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub async fn receive(&mut self) -> Result<Option<ServerMessage>, ConError> {
        if let Some(source) = &mut self.source {
            // receive is constantly being polled, therefore we need to handle
            // application pings here, once a connection is established
            let res = select! {
                biased; //if there is an incoming message it takes precedence as it might be a server ping
                m=source.next()=>Ok(m),
                p=self.ping_fsm.next(false)=>Err(p)
            };
            let next = match res {
                Ok(m) => m,
                // service ping
                Err(p) => {
                    if let Ok(p) = p {
                        if let Some(p) = p {
                            let _ = self.send_msg(p).await;
                            return Ok(None); //ping was sent. let client re-poll
                        }
                        unreachable!("This variant can't be returned by the FSM in this branch!");
                    }
                    //Timout!
                    self.disconnect().await;
                    return Err(ConError::Network);
                }
            };
            if let Some(next) = next {
                match next {
                    Ok(next) => {
                        if next.is_ping() || next.is_pong() {
                            // websocket ping/pong. this should be handled by tungstenite
                            return Ok(None);
                        }
                        // we should never get this from autopush
                        if next.is_empty() {
                            return Err(ConError::Protocol);
                        }
                        if next.is_close() {
                            self.disconnect().await;
                            return Err(ConError::Network);
                        }
                        //this would probably just be utf errors
                        let text = next.into_text()?;
                        let msg = ServerMessage::from_str(&text).map_err(|_| {
                            error!("protocol error! {}", text);
                            ConError::Protocol
                        });
                        self.inspect_message(msg).await
                    }
                    Err(e) => {
                        let e: ConError = e.into();
                        if matches!(e, ConError::Network | ConError::NetworkCritical) {
                            self.disconnect().await;
                        }
                        Err(e)
                    }
                }
            } else {
                trace!("stream closed!");
                self.disconnect().await;
                Err(ConError::Network)
            }
        } else {
            // treat missing open connection as a non-critical (ie recoverable) network error
            Err(ConError::Network)
        }
    }

    pub async fn send_msg(&mut self, msg: ClientMessage) -> Result<(), ConError> {
        if !self.ping_fsm.is_started() && !msg.is_hello() {
            error!("tried sending non-hello message before hello received!");
            return Err(ConError::Protocol);
        }
        let msg: String = msg.try_into().map_err(|_| {
            error!("Failed to serialise client message! this is a BUG");
            ConError::Protocol
        })?;
        let msg = Message::Text(msg.into());
        if let Some(sink) = &mut self.sink {
            let rsp: Result<(), ConError> = sink.send(msg).await.map_err(Into::into);
            if let Err(e) = &rsp
                && matches!(e, ConError::Network | ConError::NetworkCritical)
            {
                self.disconnect().await;
            }
            rsp
        } else {
            //if there is no sink its probably a network issue
            Err(ConError::Network)
        }
    }
    pub const fn is_connected(&self) -> bool {
        self.sink.is_some()
    }

    pub const fn accepts_requests(&self) -> bool {
        self.ping_fsm.is_started()
    }

    pub async fn close(mut self) -> Url {
        self.disconnect().await;
        self.url
    }
    async fn disconnect(&mut self) {
        if let Some(sink) = &mut self.sink {
            trace!("closing connection!");
            let _ = sink.close().await;
        }
        self.sink = None;
        self.source = None;
        //maybe the bool can be replaced by checking if the fsm is in `Stop` state?
        self.ping_fsm.reset();
    }
}
