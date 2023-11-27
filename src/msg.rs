use anyhow::{bail, Context};
use log::{debug, error};
use rand::seq::SliceRandom;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{StdoutLock, Write};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Injected {
    GossipNow,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Event<Message, Injected = ()> {
    Message(Message),
    Injected(Injected),
    EOF,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Body {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Read,
    #[serde(rename = "read_ok")]
    ReadOkEcho {
        messages: Vec<usize>,
    },
    #[serde(rename = "read_ok")]
    ReadOkCount {
        value: usize,
    },
    Add {
        delta: usize,
    },
    AddOk,
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    GossipEcho {
        ids: Vec<usize>,
    },
    GossipCount {
        adds: Vec<(String, usize, usize)>,
    },
    Generate,
    GenerateOk {
        id: String,
    },
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}
