use tokio::sync::{mpsc::Sender, oneshot};
use tracing::trace;
use zbus::interface;

use crate::{
    client::{self, RegistrationError},
    interfaces::{DARegister, DAUnregister, DRRegister},
};

pub struct Distributor {
    sender: Sender<client::DistMethods>,
}

impl Distributor {
    pub const fn new(sender: Sender<client::DistMethods>) -> Self {
        Self { sender }
    }
}

#[interface(name = "org.unifiedpush.Distributor2")]
impl Distributor {
    #[allow(clippy::unused_self)]
    async fn register(&self, args: DARegister) -> DRRegister<'_> {
        trace!(
            "called register from {} with token {}",
            args.service, args.token
        );
        let (send, recv) = oneshot::channel::<Result<(), RegistrationError>>();
        let _ = self
            .sender
            .send(client::DistMethods::Register {
                args,
                response: send,
            })
            .await;

        let msg = recv
            .await
            //assume registration failed
            .unwrap_or(Err(RegistrationError::InternalError));
        match msg {
            Err(reason) => DRRegister {
                success: "REGISTRATION_FAILED",
                reason: Some(reason.into()),
            },
            Ok(()) => DRRegister {
                success: "REGISTRATION_SUCCEEDED",
                reason: None,
            },
        }
    }

    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    async fn unregister(&self, args: DAUnregister<'_>) {
        trace!("unregistered {}", args.token);
        let _ = self
            .sender
            .send(client::DistMethods::Unregister {
                token: args.token.to_string(),
            })
            .await;
    }
}
