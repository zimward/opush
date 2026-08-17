use serde::{Deserialize, Serialize};
use zbus::{
    proxy,
    zvariant::{
        DeserializeDict, SerializeDict, Type,
        as_value::{self, optional},
    },
};

#[allow(clippy::ref_option_ref)]
#[derive(Serialize, Type)]
#[zvariant(signature = "a{sv}")]
#[serde(default)]
pub struct CAMessage<'a> {
    #[serde(with = "as_value")]
    pub token: &'a str,
    #[serde(with = "as_value")]
    pub message: &'a [u8],
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub id: Option<&'a str>,
}
#[derive(DeserializeDict, Type, Debug)]
#[zvariant(signature = "a{sv}")]
pub struct CRMessage {
    pub _id: Option<String>,
}

#[derive(SerializeDict, Type)]
#[zvariant(signature = "a{sv}")]
pub struct CAEndpoint<'a> {
    pub token: &'a str,
    pub endpoint: &'a str,
}

#[derive(SerializeDict, Type)]
#[zvariant(signature = "a{sv}")]
pub struct CAUnregistered<'a> {
    pub token: &'a str,
}

/*
* stuff we call
*/
#[proxy(
    interface = "org.unifiedpush.Connector2",
    default_path = "/org/unifiedpush/Connector",
    assume_defaults = true
)]
pub trait Connector2 {
    // Message method
    fn message(&self, args: CAMessage<'_>) -> zbus::Result<CRMessage>;

    // NewEndpoint method
    #[zbus(no_reply)]
    fn new_endpoint(&self, args: CAEndpoint<'_>) -> zbus::Result<()>;

    // Unregistered method
    #[zbus(no_reply)]
    fn unregistered(&self, args: CAUnregistered<'_>) -> zbus::Result<()>;
}

/*
* stuff we provide
*/

#[derive(Deserialize, Type, Debug)]
#[zvariant(signature = "a{sv}")]
pub struct DARegister {
    #[serde(with = "as_value")]
    pub service: String,
    #[serde(with = "as_value")]
    pub token: String,
    #[serde(default, with = "optional")]
    pub description: Option<String>,
    #[serde(default, with = "optional")]
    pub vapid: Option<String>,
}

#[derive(Serialize, Type)]
#[zvariant(signature = "a{sv}")]
pub struct DRRegister<'a> {
    #[serde(with = "as_value")]
    pub success: &'a str,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'a str>,
}

#[derive(Type, Deserialize, Debug)]
#[zvariant(signature = "a{sv}")]
pub struct DAUnregister<'a> {
    #[serde(with = "as_value")]
    pub token: &'a str,
}
