use anyhow::{bail, Context};
use rand::Rng;
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
    broadcast_ids: HashSet<usize>,
    // Other nodes from topology message and the
    // broadcast index we've sent them
    other_nodes_sent: HashMap<String, HashSet<usize>>,
    waiting_on_ok: HashMap<usize, usize>,
}

impl<'a> EchoNode<'a> {
    pub fn new(init_msg: Message, mut output: StdoutLock<'a>) -> Self {
        if let Payload::Init {
            ref node_id,
            ref node_ids,
        } = init_msg.body.payload
        {
            debug!("inside EchoNode::new");
            debug!("init_msg: {:?}", init_msg.clone());
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
            let other_nodes_sent: HashMap<String, HashSet<usize>> = HashMap::new();
            /*
            for k in node_ids.iter() {
                other_nodes_sent.insert(k.to_string(), HashSet::new());
            }
            */
            EchoNode {
                node_id: Some(node_id.clone()),
                node_msg_id: 1,
                output: output,
                broadcast_ids: HashSet::new(),
                other_nodes_sent: other_nodes_sent,
                waiting_on_ok: HashMap::new(),
            }
        } else {
            error!("Expected Init message as first message");
            panic!("")
        }
    }
    pub fn create_message(
        &mut self,
        src: String,
        dest: String,
        in_reply_to: Option<usize>,
        payload: Payload,
    ) -> Message {
        let msg_id = Some(self.node_msg_id.clone());
        let body = Body {
            msg_id,
            in_reply_to,
            payload,
        };
        self.node_msg_id += 1;
        Message { src, dest, body }
    }
    pub fn send(&mut self, msg: Message) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.output, &msg).context("serialize response to Generate")?;
        self.output
            .write_all(b"\n")
            .context("write trailing newline")?;
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
                    debug!("Need to push to broadcast_ids: {:?}", message);
                    self.broadcast_ids.insert(message);
                    debug!("Current broadcast_ids: {:?}", &self.broadcast_ids);
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
                    messages: self.broadcast_ids.clone().into_iter().collect(),
                };
                self.write_message(input.dest, input.src, input.body.msg_id, payload)?;
            }
            Payload::Topology { ref topology } => {
                debug!("Received Topology message: {:?}", input.clone());
                for (k, v) in topology.iter() {
                    if k != self.node_id.as_ref().unwrap() {
                        continue;
                    }
                    for node in v.iter() {
                        debug!("Adding to topology: {:?}", node.to_string());
                        self.other_nodes_sent
                            .insert(node.to_string(), HashSet::new());
                    }
                }
                debug!("Topology after populating: {:?}", self.other_nodes_sent);
                /*
                let mut to_remove: Vec<String> = Vec::new();
                for k in self.other_nodes_sent.keys() {
                    if topology.get(k).contains_key(k) {
                        continue;
                    }
                    to_remove.push(k.to_string());
                }
                debug!("Removing nodes no longer in Topology: {:?}", to_remove);
                for k in to_remove.iter() {
                    self.other_nodes_sent.remove(k);
                }
                */
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
            Payload::BroadcastOk { .. } => {
                debug!("received BroadcastOk");
                if let Some(in_reply_to) = input.body.in_reply_to {
                    debug!("in_reply_to: {:?}", in_reply_to);
                    let val = self.waiting_on_ok.remove(&in_reply_to);
                    if let Some(val) = val {
                        for (key, seen) in self.other_nodes_sent.iter_mut() {
                            if *key != input.src {
                                continue;
                            }
                            debug!("Adding {:?} to seen for  {:?}", val, *key);
                            seen.insert(val);
                        }
                    } else {
                        debug!("Message: {:?}", input);
                        debug!("Received in_reply_to value not in waiting_on_ok, ignoring");
                    }
                } else {
                    debug!("Message: {:?}", input);
                    bail!("Expected in_reply_to in BroadcastOk")
                }
            }
            Payload::ReadOk { .. } => bail!("received ReadOk message"),
            Payload::TopologyOk { .. } => bail!("received TopologyOk message"),
        }

        debug!("Checking if we need to propagate broadcast messages");
        if self.other_nodes_sent.keys().len() != 0
            && self.broadcast_ids.len()
                != self
                    .other_nodes_sent
                    .values()
                    .map(|x| x.len())
                    .min()
                    .context("should have populated hashset already")?
        {
            let mut rng = rand::thread_rng();
            if rng.gen_range(0..100) == 0 {
                debug!("Need to propagate broadcast messages");
                let _ = self.propagate_broadcast_messages();
            }
            // Need to broadcast some messages to other nodes in topology
        }

        Ok(())
    }

    fn propagate_broadcast_messages(&mut self) -> anyhow::Result<()> {
        // let mut msgs: Vec<Message> = Vec::new();
        let mut key_ids_to_send: HashMap<String, Vec<usize>> = HashMap::new();

        for (key, val) in self.other_nodes_sent.iter() {
            // don't send messages to ourselves
            if key == self.node_id.as_ref().unwrap() {
                self.node_msg_id += 1;
                continue;
            }
            let ids_to_send: Vec<_> = self.broadcast_ids.difference(&val).cloned().collect();
            // have already sent all messages
            if ids_to_send.len() == 0 {
                continue;
            }
            key_ids_to_send.insert(key.to_string(), ids_to_send);
            /*
            debug!("Need to send ids: {:?}", ids_to_send.len());
            for id in ids_to_send.iter() {
                self.waiting_on_ok.insert(self.node_msg_id, **id);
                msgs.push(self.create_message(
                    self.node_id.clone().unwrap(),
                    key.clone(),
                    None,
                    Payload::Broadcast { message: **id },
                ));
            }
            */
        }
        debug!("Waiting on oks for: {:?}", &self.waiting_on_ok);
        debug!("Need to send broadcast ids: {:?}", &key_ids_to_send);
        let mut msgs: Vec<Message> = Vec::new();
        for (key, val) in key_ids_to_send.iter() {
                for id in val.iter() {
                    self.waiting_on_ok.insert(self.node_msg_id, *id);
                    let msg = self.create_message(
                        self.node_id.clone().unwrap(),
                        key.clone(),
                        None,
                        Payload::Broadcast { message: *id },
                    );
                    self.send(msg)?;
            }
        }
        /*
        for msg in msgs.iter() {
            self.send(msg.clone())?;
        }
        */
        /*
        for (_key, val) in self.other_nodes_sent.iter_mut() {
            *val = self.broadcast_ids.clone();
        }
        */
        Ok(())
    }
}
