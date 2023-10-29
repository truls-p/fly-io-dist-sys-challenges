use anyhow::{bail, Context};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
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

pub struct EchoNode<'a> {
    node_id: Option<String>,
    node_msg_id: usize,
    output: StdoutLock<'a>,
    broadcast_ids: Vec<usize>,
    // Other nodes from topology message and the
    // broadcast index we've sent them
    other_nodes_sent: HashMap<String, usize>,
    min_index: usize,
}

impl<'a> EchoNode<'a> {
    pub fn new(init_msg: Message, mut output: StdoutLock<'a>) -> Self {
        if let Payload::Init { node_id, node_ids } = init_msg.body.payload {
            debug!("inside EchoNode::new");
            let reply = Message {
                src: init_msg.dest,
                dest: init_msg.src,
                body: Body {
                    msg_id: Some(0),
                    in_reply_to: init_msg.body.msg_id,
                    payload: Payload::InitOk,
                },
            };
            serde_json::to_writer(&mut output, &reply)
                .context("serialize response to init")
                .unwrap();
            output
                .write_all(b"\n")
                .context("write trailing newline")
                .unwrap();
            let mut other_nodes_sent: HashMap<String, usize> = HashMap::new();
            for k in node_ids.iter() {
                other_nodes_sent.insert(k.to_string(), 0);
            }
            EchoNode {
                node_id: Some(node_id.clone()),
                node_msg_id: 1,
                output: output,
                broadcast_ids: Vec::new(),
                other_nodes_sent: other_nodes_sent,
                min_index: 0,
            }
        } else {
            error!("Expected Init message as first message");
            panic!("")
        }
    }
    pub fn create_message(
        &self,
        src: String,
        dest: String,
        in_reply_to: Option<usize>,
        payload: Payload,
    ) -> Message {
        let msg_id = Some(self.node_msg_id);
        let body = Body {
            msg_id,
            in_reply_to,
            payload,
        };
        Message { src, dest, body }
    }
    pub fn send(&mut self, msg: Message) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.output, &msg).context("serialize response to Generate")?;
        self.output
            .write_all(b"\n")
            .context("write trailing newline")?;
        self.node_msg_id += 1;
        Ok(())
    }
    pub fn write_message(
        &mut self,
        src: String,
        dest: String,
        in_reply_to: Option<usize>,
        payload: Payload,
    ) -> anyhow::Result<()> {
        let msg = self.create_message(src, dest, in_reply_to, payload);
        self.send(msg)
    }
    pub fn step(&mut self, input: Message) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init { .. } => {
                bail!("Should've already processed init message");
            }
            Payload::Echo { echo } => {
                self.write_message(
                    input.dest,
                    input.src,
                    input.body.msg_id,
                    Payload::EchoOk { echo },
                )?;
            }
            Payload::Generate { .. } => {
                let id = Uuid::new_v4();
                let payload = Payload::GenerateOk { id: id.to_string() };
                self.write_message(input.dest, input.src, input.body.msg_id, payload)?;
            }
            Payload::Broadcast { message } => {
                if !self.broadcast_ids.contains(&message) {
                    dbg!("Need to push {message} to broadcast_ids");
                    self.broadcast_ids.push(message);
                }
                self.write_message(
                    input.dest,
                    input.src,
                    input.body.msg_id,
                    Payload::BroadcastOk,
                )?;
            }
            Payload::Read { .. } => {
                let payload = Payload::ReadOk {
                    messages: self.broadcast_ids.clone(),
                };
                self.write_message(input.dest, input.src, input.body.msg_id, payload)?;
            }
            Payload::Topology { topology } => {
                for k in topology.keys() {
                    if self.other_nodes_sent.contains_key(k) {
                        continue;
                    }
                    self.other_nodes_sent.insert(k.to_string(), 0);
                }
                self.write_message(
                    input.dest,
                    input.src,
                    input.body.msg_id,
                    Payload::TopologyOk,
                )?;
            }
            Payload::EchoOk { .. } => {}
            Payload::InitOk { .. } => bail!("received InitOk message"),
            Payload::GenerateOk { .. } => bail!("received GenerateOk message"),
            Payload::BroadcastOk { .. } => {}
            Payload::ReadOk { .. } => bail!("received ReadOk message"),
            Payload::TopologyOk { .. } => bail!("received TopologyOk message"),
        }

        if self.min_index < self.broadcast_ids.len() && self.other_nodes_sent.keys().len() != 0 {
            info!("Need to propagate broadcast messages");
            let _ = self.propagate_broadcast_messages();
            // Need to broadcast some messages to other nodes in topology
        }

        Ok(())
    }

    fn propagate_broadcast_messages(&mut self) -> anyhow::Result<()> {
        let mut msgs: Vec<Message> = Vec::new();
        for (key, val) in self.other_nodes_sent.iter() {
            // don't send messages to ourselves
            if key == self.node_id.as_ref().unwrap() {
                self.node_msg_id += 1;
                continue;
            }
            // have already sent all messages
            if *val >= self.broadcast_ids.len() {
                continue;
            }
            let ids_to_send = &self.broadcast_ids[*val..];
            for id in ids_to_send {
                msgs.push(self.create_message(
                    self.node_id.clone().unwrap(),
                    key.clone(),
                    None,
                    Payload::Broadcast { message: *id },
                ));
            }
        }
        for msg in msgs {
            self.send(msg)?;
        }
        for (_key, val) in self.other_nodes_sent.iter_mut() {
            *val = self.broadcast_ids.len();
        }
        Ok(())
    }
}
