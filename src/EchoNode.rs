use crate::msg::{Body, Event, Injected, Message, Payload};
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

pub struct EchoNode<'a> {
    node_id: Option<String>,
    node_msg_id: usize,
    output: StdoutLock<'a>,
    broadcast_ids: HashSet<usize>,
    // Other nodes from topology message and the
    // broadcast index we've sent them
    other_nodes_seen: HashMap<String, HashSet<usize>>,
}

impl<'a> EchoNode<'a> {
    pub fn new(
        init_msg: Event<Message, Injected>,
        mut output: StdoutLock<'a>,
        tx: std::sync::mpsc::Sender<Event<Message, Injected>>,
    ) -> Self {
        debug!("in EchoNode::new");
        match init_msg {
            Event::EOF => {
                panic!("expected init")
            }
            Event::Injected(..) => {
                panic!("expected init")
            }
            Event::Message(init_msg) => {
                if let Payload::Init {
                    ref node_id,
                    node_ids: _,
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
                    let other_nodes_seen: HashMap<String, HashSet<usize>> = HashMap::new();
                    thread::spawn(move || loop {
                        std::thread::sleep(Duration::from_millis(30));
                        tx.send(Event::Injected(Injected::GossipNow)).unwrap();
                    });

                    /*
                    for k in node_ids.iter() {
                        other_nodes_seen.insert(k.to_string(), HashSet::new());
                    }
                    */
                    EchoNode {
                        node_id: Some(node_id.clone()),
                        node_msg_id: 1,
                        output: output,
                        broadcast_ids: HashSet::new(),
                        other_nodes_seen: other_nodes_seen,
                    }
                } else {
                    error!("Expected Init message as first message");
                    panic!("")
                }
            }
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
    pub fn step(&mut self, input: Event<Message, Injected>) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Message(input) => match input.body.payload {
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
                        // debug!("Need to push to broadcast_ids: {:?}", message);
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
                    let payload = Payload::ReadOkEcho {
                        messages: self.broadcast_ids.clone().into_iter().collect(),
                    };
                    self.write_message(input.dest, input.src, input.body.msg_id, payload)?;
                }
                Payload::Topology { ref topology } => {
                    debug!("Received Topology message: {:?}", input.clone());
                    if self.node_id.clone().unwrap() == "n0" {
                        debug!("n0 so adding all");
                        for (k, _v) in topology.iter() {
                            /*
                            if k != self.node_id.as_ref().unwrap() {
                                continue;
                            }
                            */
                            self.other_nodes_seen.insert(k.to_string(), HashSet::new());
                            /*
                            for node in v.iter() {
                                debug!("Adding to topology: {:?}", node.to_string());
                                self.other_nodes_seen
                                    .insert(node.to_string(), HashSet::new());
                            }
                            */
                        }
                    } else {
                        debug!("not n0 so adding only n0");
                        self.other_nodes_seen
                            .insert("n0".to_string(), HashSet::new());
                    }
                    debug!("Topology after populating: {:?}", self.other_nodes_seen);
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
                Payload::GossipCount { .. } => bail!("EchoNode received GossipCount message"),
                Payload::GossipEcho { ids } => {
                    debug!("received gossip: {:?}, ids: {:?}", &input.src, ids.clone());
                    self.broadcast_ids.extend(ids.clone());
                    self.other_nodes_seen
                        .get_mut(&input.src)
                        .unwrap()
                        .extend(ids);
                    debug!("other_nodes_seen: {:?}", self.other_nodes_seen);
                }
                Payload::ReadOkEcho { .. } => bail!("received ReadOk message"),
                Payload::ReadOkCount { .. } => bail!("received ReadOk message"),
                Payload::Add { .. } => bail!("received Add message for EchoNode"),
                Payload::AddOk { .. } => bail!("received AddOk message"),
                Payload::TopologyOk { .. } => bail!("received TopologyOk message"),
            },
            Event::Injected(_input) => {
                let _ = self.propagate_broadcast_messages();
            }
        }

        Ok(())
    }

    fn propagate_broadcast_messages(&mut self) -> anyhow::Result<()> {
        for key in self
            .other_nodes_seen
            .keys()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>()
            .iter()
        {
            // don't send messages to ourselves
            if key == self.node_id.as_ref().unwrap() {
                continue;
            }
            debug!("working on: {:?}", key);
            // let mut ids = self.broadcast_ids.iter().cloned().collect::<Vec<_>>();
            let ids = self.broadcast_ids.clone();
            debug!("ids: {:?}", ids);
            let seen = self.other_nodes_seen.get(key).unwrap();
            if *seen == ids {
                debug!("No need to send gossip, ids and seen the same");
                continue;
            }
            debug!("seen: {:?}", seen);
            let ids: Vec<_> = ids.difference(seen).cloned().collect();
            let mut rng = &mut rand::thread_rng();
            let extra: Vec<_> = self
                .broadcast_ids
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .choose_multiple(&mut rng, self.broadcast_ids.len() / 10)
                .cloned()
                .collect();
            debug!("extra: {:?}", extra);
            let mut ids_to_send = ids.iter().cloned().collect::<Vec<_>>();
            ids_to_send.extend(extra.iter());
            ids_to_send.sort();
            ids_to_send.dedup();
            debug!("ids_to_send: {:?}", ids_to_send);
            let msg = self.create_message(
                self.node_id.clone().unwrap(),
                key.clone(),
                None,
                Payload::GossipEcho { ids: ids_to_send },
            );
            self.send(msg)?;
        }
        Ok(())
    }
}
