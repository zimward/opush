// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2026, MPL autopush-rs https://github.com/mozilla-services/autopush-rs/
//
// Alternatively, the contents of this file may be used under the terms
// of the GNU General Public License Version 3, as described below:
//
// This file is free software: you may copy, redistribute and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.
//
// This file is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
// Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see http://www.gnu.org/licenses/.
//
//! Definition of Internal Router and Websocket protocol messages
//!
//! This module is a structured definition of several protocol. Both
//! messages received from the client and messages sent from the server are
//! defined here. The `derive(Deserialize)` and `derive(Serialize)` annotations
//! are used to generate the ability to serialize these structures to JSON,
//! using the `serde` crate. More docs for serde can be found at
//! <https://serde.rs>
use std::collections::HashMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use tracing::trace;
use uuid::Uuid;

#[derive(Default, Deserialize, Clone, Debug)]
/// A Publishable Notification record. This is a notification that is either
/// received from a third party or is outbound to a `UserAgent`.
///
pub struct Notification {
    // Required values
    #[serde(rename = "channelID")]
    pub channel_id: Uuid,
    pub version: String,
    // Optional values, which imply a "None" default.
    pub data: Option<String>,
    // pub headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "messageType", rename_all = "snake_case")]
pub enum ClientMessage {
    Hello {
        uaid: Option<Uuid>,
        //apparently autopush doesn't use this field, so lets save some bandwidth
        // #[serde(rename = "channelIDs", skip_serializing_if = "Option::is_none")]
        // channel_ids: Option<Vec<Uuid>>,
        // #[serde(skip_serializing_if = "Option::is_none")]
        // broadcasts: Option<HashMap<String, String>>,
    },

    Register {
        #[serde(rename = "channelID")]
        channel_id: String,
        key: Option<String>,
    },

    Unregister {
        #[serde(rename = "channelID")]
        channel_id: String,
        code: Option<u32>,
    },

    // BroadcastSubscribe,
    Ack {
        updates: Vec<ClientAck>,
    },

    Nack {
        #[serde(skip_serializing_if = "Option::is_none")]
        code: Option<i32>,
        version: String,
    },

    Ping,
}

impl ClientMessage {
    pub const fn is_hello(&self) -> bool {
        matches!(self, Self::Hello { .. })
    }
}

impl TryInto<String> for ClientMessage {
    type Error = serde_json::error::Error;

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            Self::Ping => Ok("{}".to_string()),
            e => serde_json::to_string(&e),
        }
    }
}

/// Returned `ACKnowledgement` of the received message by the User Agent.
/// This is the payload for the `messageType:ack` packet.
///
#[derive(Debug, Serialize)]
pub struct ClientAck {
    /// The `channel_id` which received messages
    #[serde(rename = "channelID")]
    pub channel_id: Uuid,
    /// The corresponding version number for the message.
    pub version: String,
    /// An optional code categorizing the status of the ACK
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "messageType", rename_all = "snake_case")]
pub enum ServerMessage {
    Hello {
        uaid: Uuid,
        status: u32,
        // This is required for output, but will always be "true"
        use_webpush: bool,
    },

    Register {
        #[serde(rename = "channelID")]
        channel_id: Uuid,
        status: u32,
        #[serde(rename = "pushEndpoint")]
        push_endpoint: String,
    },

    Unregister {
        #[serde(rename = "channelID")]
        channel_id: Uuid,
        #[allow(dead_code)]
        status: u32,
    },

    Broadcast,

    Notification(Notification),

    Ping,
}

impl FromStr for ServerMessage {
    type Err = serde_json::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        trace!("Raw Server event: {s}");
        serde_json::from_str::<HashMap<(), ()>>(s)
            .map(|_| Self::Ping)
            .or_else(|_| serde_json::from_str(s))
    }
}
