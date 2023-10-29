use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{StdoutLock, Write};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Body {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Broadcast {
        message: usize,
    },
    BroadcastOk,
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

pub struct EchoNode {
    node_id: Option<String>,
    node_msg_id: usize,
    broadcast_ids: Vec<usize>,
    // Other nodes from topology message and the
    // broadcast index we've sent them
    other_nodes_sent: HashMap<String, usize>,
    min_index: usize,
}

impl EchoNode {
    pub fn new() -> Self {
        EchoNode {
            node_id: None,
            node_msg_id: 0,
            broadcast_ids: Vec::new(),
            other_nodes_sent: HashMap::new(),
            min_index: 0,
        }
    }
    pub fn step(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init { node_id, node_ids } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.node_msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::InitOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to init")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.node_msg_id += 1;
                self.node_id = Some(node_id.clone());
                for k in node_ids.iter() {
                    if self.other_nodes_sent.contains_key(k) {
                        continue;
                    }
                    self.other_nodes_sent.insert(k.to_string(), 0);
                }
            }
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.node_msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Echo")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.node_msg_id += 1;
            }
            Payload::Generate { .. } => {
                let id = Uuid::new_v4();
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.node_msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::GenerateOk { id: id.to_string() },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Generate")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.node_msg_id += 1;
            }
            Payload::Broadcast { message } => {
                if !self.broadcast_ids.contains(&message) {
                    dbg!("Need to push {message} to broadcast_ids");
                    self.broadcast_ids.push(message);
                }
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.node_msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::BroadcastOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Broadcast")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.node_msg_id += 1;
            }
            Payload::Read { .. } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.node_msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::ReadOk {
                            messages: self.broadcast_ids.clone(),
                        },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Read")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.node_msg_id += 1;
            }
            Payload::Topology { topology } => {
                for k in topology.keys() {
                    if self.other_nodes_sent.contains_key(k) {
                        continue;
                    }
                    self.other_nodes_sent.insert(k.to_string(), 0);
                }
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.node_msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::TopologyOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Topology")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.node_msg_id += 1;
            }
            Payload::EchoOk { .. } => {}
            Payload::InitOk { .. } => bail!("received InitOk message"),
            Payload::GenerateOk { .. } => bail!("received GenerateOk message"),
            Payload::BroadcastOk { .. } => {}
            Payload::ReadOk { .. } => bail!("received ReadOk message"),
            Payload::TopologyOk { .. } => bail!("received TopologyOk message"),
        }

        if self.min_index < self.broadcast_ids.len() && self.other_nodes_sent.keys().len() != 0 {
            // Need to broadcast some messages to other nodes in topology
            dbg!("Need to send broadcast messages");
            dbg!(
                "min_index: {}, broadcast_ids: {}, other_nodes_sent: {}",
                &self.min_index,
                &self.broadcast_ids,
                &self.other_nodes_sent
            );
            for (key, val) in self.other_nodes_sent.iter_mut() {
                dbg!("Sending broadcast to: {}", key);
                if *val < self.broadcast_ids.len() {
                    let ids_to_send = &self.broadcast_ids[*val..];
                    dbg!("Need to send the following ids: {}", ids_to_send);
                    for id in ids_to_send {
                        if key == self.node_id.as_ref().unwrap() {
                            dbg!("don't send same value to oneself");
                            self.node_msg_id += 1;
                            continue;
                        }
                        dbg!("Sending broadcast to: {}, with val: {}", key, *val);
                        let broadcast = Message {
                            src: self.node_id.clone().unwrap(),
                            dest: key.clone(),
                            body: Body {
                                msg_id: Some(self.node_msg_id),
                                in_reply_to: None,
                                payload: Payload::Broadcast { message: *id },
                            },
                        };
                        dbg!(broadcast.clone());
                        serde_json::to_writer(&mut *output, &broadcast)
                            .context("serialize response to Echo")?;
                        output.write_all(b"\n").context("write trailing newline")?;
                        self.node_msg_id += 1;
                    }
                    *val = self.broadcast_ids.len();
                }
            }
        }

        Ok(())
    }
}
