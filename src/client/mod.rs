use std::{collections::HashMap, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{mpsc::Receiver, oneshot},
};
use tracing::{error, info, trace, warn};
use uuid::Uuid;
use zbus::Connection;

use crate::{
    client::{
        protocol::{ClientAck, ClientMessage, Notification, ServerMessage},
        ws::{ConError, WsCon},
    },
    interfaces::{Connector2Proxy, DARegister},
};

mod protocol;
mod ws;

#[derive(Debug)]
pub enum DistMethods {
    //sent by the distributor
    Register {
        args: DARegister,
        response: oneshot::Sender<Result<(), RegistrationError>>,
    },
    Unregister {
        token: String,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum RegistrationError {
    InternalError,
    Network,
    ActionRequired, //maybe unused
    // VapidRequired, //nevere returned
    Unauthorized, //maybe unused
}
impl From<RegistrationError> for &'static str {
    fn from(val: RegistrationError) -> Self {
        match val {
            RegistrationError::InternalError => "INTERNAL_ERROR",
            RegistrationError::Network => "NETWORK",
            RegistrationError::ActionRequired => "ACTION_REQUIRED",
            RegistrationError::Unauthorized => "UNAUTHORIZED",
        }
    }
}

type Token = String;
type Path = String;
type PushUrl = String;

#[derive(Debug, Serialize, Deserialize)]
struct Registration {
    token: Token,
    path: Path,
    channel_url: Option<PushUrl>,
    description: Option<String>,
    vapid: Option<String>,
    messages_delivered: usize,
}

impl Registration {
    pub const fn message_delivered(&mut self) {
        self.messages_delivered += 1;
    }
}

async fn dbus_proxy(con: &Connection, dest: String) -> Result<Connector2Proxy<'_>, zbus::Error> {
    Connector2Proxy::builder(con)
        .destination(dest)?
        .build()
        .await
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientState {
    uaid: Uuid,
    registrations: HashMap<Uuid, Registration>,
}

pub type Url = String;

#[derive(Debug)]
pub struct Client {
    ws: WsCon,
    con: Connection,
    uaid: Option<Uuid>,
    registrations: HashMap<Uuid, Registration>,
    pending: HashMap<Uuid, oneshot::Sender<Result<(), RegistrationError>>>,
}

// connection state model should just assume we might be disconnect at every step
// and then dump the client state at the very end of the program's lifetime.
// if a reregistration is needed etc we can just log it
impl Client {
    pub fn new(url: Url, state: Option<ClientState>, con: Connection) -> Self {
        let (uaid, registrations) = if let Some(ClientState {
            uaid,
            registrations,
        }) = state
        {
            (Some(uaid), registrations)
        } else {
            (None, HashMap::new())
        };

        Self {
            ws: WsCon::new(url),
            con,
            uaid,
            registrations,
            pending: HashMap::new(),
        }
    }

    // simplifies the select! and also nests another service loop for the reconnect timeout loop
    async fn process_connect_res(
        res: Result<(), ConError>,
        registrations: &mut HashMap<Uuid, Registration>,
        dist_receiver: &mut Receiver<DistMethods>,
    ) -> Result<(), ()> {
        match res {
            Ok(()) => Ok(()),
            //no further reconnect attempts
            Err(ConError::NetworkCritical) => {
                error!("Critical network error!. Exiting...");
                Err(())
            }
            Err(_) => {
                error!("Reconnect failed, retrying in 5s...");
                let delay = tokio::time::sleep(Duration::from_secs(5));
                tokio::pin!(delay);
                //keep serviceing dbus while waiting for the backoff delay
                loop {
                    select! {
                        biased;
                        ()=&mut delay=>{
                            return Ok(());
                        }
                        ()=Self::service_dbus_neterror(registrations,dist_receiver)=>{}
                    }
                }
            }
        }
    }
    /// Reply to all Register dbus requests with a network error immediately
    async fn service_dbus_neterror(
        registrations: &mut HashMap<Uuid, Registration>,
        dist_receiver: &mut Receiver<DistMethods>,
    ) {
        let req = dist_receiver.recv().await;
        //no more async after this point. so cancel safe
        match req {
            Some(DistMethods::Register { response, .. }) => {
                let _ = response.send(Err(RegistrationError::Network));
            }
            Some(DistMethods::Unregister { token }) => {
                //PERFORMANCE: this is O(cap)
                registrations.retain(|_, v| v.token != token);
                // unregistrations will be done if the channel on the next message that channel deliveres
                // as it can't be related to any known one
            }
            None => (),
        }
    }

    pub async fn poll(&mut self, dist_receiver: &mut Receiver<DistMethods>) -> Result<(), ()> {
        if !self.ws.is_connected() {
            //drop all pending registrations.
            for (_, v) in self.pending.drain() {
                let _ = v.send(Err(RegistrationError::Network));
            }

            let con_future = self.ws.connect(ClientMessage::Hello { uaid: self.uaid });
            tokio::pin!(con_future);
            loop {
                select! {
                    ()=Self::service_dbus_neterror(&mut self.registrations,dist_receiver)=>{}
                    // if connect failed back off and retry
                    res=&mut con_future=>{return Self::process_connect_res(res,&mut self.registrations,dist_receiver).await}
                }
            }
        }
        select! {
                req= dist_receiver.recv()=>
                {
                    if let Some(req)=req{
                        match req {
                            DistMethods::Register { args,response } => {
                                self.register(args,response).await;
                            },
                            DistMethods::Unregister { token } => {
                                self.unregister(token).await;
                            },
                        }
                    }else{
                        error!("all dbus sender channels dropped!. exiting...");
                        return Err(());
                    }
                }
                msg = self.ws.receive()=>self.handle_server_msg( msg).await,

        };
        Ok(())
    }

    async fn handle_server_msg(&mut self, msg: Result<Option<ServerMessage>, ConError>) {
        match msg {
            Ok(msg) => {
                let Some(msg) = msg else {
                    return;
                };
                trace!("Received Event {:?}", msg);
                //handle the actual messages
                match msg {
                    ServerMessage::Hello { uaid, .. } => {
                        trace!("server hello received with uaid {uaid}");
                        if let Some(current_uaid) = self.uaid
                            && current_uaid != uaid
                        {
                            info!("UAID changed. reregistering");
                            for reg in self.registrations.values_mut() {
                                reg.channel_url = None;
                            }
                        }
                        self.uaid = Some(uaid);
                        for (uuid, reg) in &mut self.registrations {
                            if reg.channel_url.is_none() {
                                let res = self
                                    .ws
                                    .send_msg(ClientMessage::Register {
                                        channel_id: uuid.to_string(),
                                        key: reg.vapid.clone(),
                                    })
                                    .await;
                                if res.is_err() {
                                    error!("Reregistration of {} failed!", reg.path);
                                }
                            }
                        }
                    }
                    ServerMessage::Register {
                        channel_id,
                        status,
                        push_endpoint,
                    } => {
                        self.handle_register(channel_id, push_endpoint, status)
                            .await;
                    }
                    ServerMessage::Unregister { channel_id, .. } => {
                        self.handle_unregistered(channel_id).await;
                    }
                    ServerMessage::Notification(notification) => {
                        match self.handle_notif(notification).await {
                            Err(version) => {
                                trace!("sending nack for version: {}", version);
                                let _ = self
                                    .ws
                                    .send_msg(ClientMessage::Nack {
                                        version,
                                        code: Some(301),
                                    })
                                    .await;
                            }
                            Ok((msgid, version)) => {
                                trace!("sending ack for {}\n version: {}", msgid, version);
                                let ack = ClientAck {
                                    channel_id: msgid,
                                    version,
                                    code: Some(100),
                                };
                                let _ = self
                                    .ws
                                    .send_msg(ClientMessage::Ack {
                                        updates: Vec::from([ack]),
                                    })
                                    .await;
                            }
                        }
                    }
                    ServerMessage::Ping => {
                        unreachable!("Ping messages get handled by the WsCon")
                    }
                    ServerMessage::Broadcast => {
                        error!("Received Broadcast message. How did we get here? THIS IS A BUG");
                    } //we should never get this
                }
            }
            Err(e) => {
                error!("Network error: {:?}", e);
            }
        }
    }
    async fn handle_register(&mut self, channel_id: Uuid, push_endpoint: String, status: u32) {
        // send the dbus registration response for the distributor
        // if none is returned its a reregistration
        if let Some(resp) = self.pending.remove(&channel_id) {
            if status == 200 {
                let _ = resp.send(Ok(()));
            } else {
                let _ = resp.send(Err(RegistrationError::InternalError));
                let _ = self.registrations.remove(&channel_id);
                return;
            }
        //tell client it got unregistered if it wasn't a new registration
        } else if status != 200
            && let Some(reg) = self.registrations.remove(&channel_id)
        {
            info!("Reregistration of {} failed! unregistering...", reg.path);
            if let Ok(proxy) = dbus_proxy(&self.con, reg.path.clone()).await
                && proxy
                    .unregistered(crate::interfaces::CAUnregistered { token: &reg.token })
                    .await
                    .is_err()
            {
                error!("Failed to call unregister for {}", reg.path);
            }
            return;
        }

        // this is the expected path
        if let Some(reg) = self.registrations.get_mut(&channel_id) {
            if let Ok(proxy) = dbus_proxy(&self.con, reg.path.clone()).await
                && proxy
                    .new_endpoint(crate::interfaces::CAEndpoint {
                        token: &reg.token,
                        endpoint: &push_endpoint,
                    })
                    .await
                    .is_err()
            {
                error!("Failed to call NewEndpoint for {}", reg.path);
            }
            // save the channel url even if we couldn't deliver it so that its cached
            reg.channel_url = Some(push_endpoint);
        } else {
            //this might happen after a connection loss during registrations
            warn!(
                "Received endpoint thats not associated with any channel. channel: {}",
                channel_id
            );
            //tell autopush to unregister it to keep its state clean
            let _ = self
                .ws
                .send_msg(ClientMessage::Unregister {
                    channel_id: channel_id.to_string(),
                    code: None,
                })
                .await;
        }
    }
    async fn handle_unregistered(&mut self, channel_id: Uuid) {
        if let Some(reg) = self.registrations.remove(&channel_id)
            && let Ok(proxy) = dbus_proxy(&self.con, reg.path.clone()).await
            && proxy
                .unregistered(crate::interfaces::CAUnregistered { token: &reg.token })
                .await
                .is_err()
        {
            error!("Failed to call Unregister for {}", reg.path);
        }
        // if no entry for it was present it was a late unregister due to connection loss. etc
    }
    async fn handle_notif(&mut self, notification: Notification) -> Result<(Uuid, String), String> {
        if let Some(reg) = self.registrations.get_mut(&notification.channel_id) {
            info!(
                "sending message to client {}, from channel {}",
                reg.path, notification.channel_id
            );
            let data = notification.data.unwrap_or_default();
            let mut output: Vec<u8> = Vec::with_capacity(data.len());
            if URL_SAFE_NO_PAD.decode_vec(&data, &mut output).is_err() {
                return Err(notification.version);
            }
            if let Ok(proxy) = dbus_proxy(&self.con, reg.path.clone()).await
                && proxy
                    .message(crate::interfaces::CAMessage {
                        token: &reg.token,
                        message: &output,
                        id: None,
                    })
                    .await
                    .is_ok()
            {
                info!("delivered message for {}", reg.path);
                reg.message_delivered();
                Ok((notification.channel_id, notification.version))
            } else {
                error!("Failed to deliver notification to {}", reg.path);
                Err(notification.version)
            }
        } else {
            warn!(
                "got notif for unknown channel {}. Unregistering...",
                notification.channel_id
            );
            let _ = self
                .ws
                .send_msg(ClientMessage::Unregister {
                    channel_id: notification.channel_id.to_string(),
                    code: None,
                })
                .await;
            Err(notification.version)
        }
    }

    // register a new channel for a client and return a polling future to indicate success
    pub async fn register(
        &mut self,
        args: DARegister,
        response: oneshot::Sender<Result<(), RegistrationError>>,
    ) {
        // we can't register yet
        if !self.ws.accepts_requests() {
            let _ = response.send(Err(RegistrationError::Network));
            return;
        }
        // check if there exists a registration for the client already, otherwise create a new uuid
        // this is technically not quite optimal since we first do a linear search through the map and then another lookup
        // but the map will never be big anyways and it simplifies the controlflow a lot
        let uuid = self
            .registrations
            .iter()
            .find(|(_, e)| e.token == args.token)
            .map_or_else(Uuid::new_v4, |(uuid, _)| *uuid);

        let reg = self.registrations.remove(&uuid).unwrap_or(Registration {
            token: args.token,
            path: args.service,
            channel_url: None,
            description: args.description,
            vapid: args.vapid,
            messages_delivered: 0,
        });

        if let Some(ref url) = reg.channel_url {
            //registration exists already
            let _ = response.send(Ok(()));
            // send url to client
            if let Ok(proxy) = dbus_proxy(&self.con, reg.path.clone()).await {
                let _ = proxy
                    .new_endpoint(crate::interfaces::CAEndpoint {
                        token: &reg.token,
                        endpoint: url,
                    })
                    .await;
            } else {
                error!("Failed to call NewEndpoint for {}", reg.path);
            }
            // re-insert the registration as we removed it from the list earlier
            // to keep the url cached even if delivery failed
            self.registrations.insert(uuid, reg);
            return;
        }

        // nothing cached. actually send register request to the server
        if let Err(e) = self
            .ws
            .send_msg(ClientMessage::Register {
                channel_id: uuid.to_string(),
                key: reg.vapid.clone(),
            })
            .await
        {
            let e = match e {
                ConError::Network | ConError::NetworkCritical => RegistrationError::Network,
                ConError::Permission => RegistrationError::Unauthorized,
                ConError::Protocol => RegistrationError::InternalError,
            };
            let _ = response.send(Err(e));
            // drops registration completely here as we failed to send the request
        } else {
            // save the registration and return channel as we will need to wait for the response
            self.registrations.insert(uuid, reg);
            if let Some(resp) = self.pending.insert(uuid, response) {
                warn!("register for the same uuid got called twice!");
                let _ = resp.send(Err(RegistrationError::InternalError));
            }
        }
    }

    //reverse of register
    pub async fn unregister(&mut self, token: String) {
        // Distributro2.unregister can't return anything, so not much point checking the return
        // should never have more than 1 entry
        if let Some((uuid, reg)) = self
            .registrations
            .iter()
            .find(|(_k, v)| v.token == token)
            .map(|(k, v)| (*k, v))
        {
            if self
                .ws
                .send_msg(ClientMessage::Unregister {
                    channel_id: uuid.to_string(),
                    code: None,
                })
                .await
                .is_err()
                && let Ok(proxy) = dbus_proxy(&self.con, reg.path.clone()).await
                && proxy
                    .unregistered(crate::interfaces::CAUnregistered { token: &token })
                    .await
                    .is_ok()
            {
                warn!(
                    "Failed to unregister {}. Dropped registration instead",
                    reg.path
                );
                self.registrations.remove(&uuid);
            }
        } else {
            warn!("Unregister called for unknown token {}", token);
        }
    }

    pub async fn close(self) -> (Url, Option<ClientState>) {
        let url = self.ws.close().await;
        (
            url,
            (if let Some(uaid) = self.uaid {
                Some(ClientState {
                    uaid,
                    registrations: self.registrations,
                })
            } else {
                None
            }),
        )
    }
}
