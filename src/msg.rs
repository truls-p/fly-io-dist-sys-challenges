
use serde::{Serialize, Deserialize};
use anyhow::{Context, bail};
use std::io::{StdoutLock, Write};
use std::collections::{HashMap};
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
    Topology { topology: HashMap<String, Vec<String>> },
    TopologyOk,
    Read,
    ReadOk { messages: Vec<usize> },
    Broadcast { message: usize },
    BroadcastOk,
    Generate,
    GenerateOk { id: String },
    Echo { echo: String },
    EchoOk { echo: String },
    Init { node_id: String, node_ids: Vec<String>, },
    InitOk,
}

pub struct EchoNode {
    id: usize,
    broadcast_ids: Vec::<usize>,
}

impl EchoNode {
    pub fn new() -> Self {
        EchoNode { id: 0, broadcast_ids: Vec::new() }
    }
    pub fn step(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init { .. } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::InitOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to init")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;

            },
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::EchoOk { echo },
                    }
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to Echo")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            },
            Payload::Generate{ .. } => {
                let id = Uuid::new_v4();
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::GenerateOk{ id: id.to_string() },
                    }
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to Echo")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            },
            Payload::Broadcast{ message } => {
                self.broadcast_ids.push(message);
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::BroadcastOk,
                    }
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to Echo")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            },
            Payload::Read{ .. } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::ReadOk{ messages: self.broadcast_ids.clone() },
                    }
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to Echo")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            },
            Payload::Topology { topology } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::TopologyOk,
                    }
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to Echo")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            },
            Payload::EchoOk { .. } => {},
            Payload::InitOk { .. } => bail!("received InitOk message"), 
            Payload::GenerateOk { .. } => bail!("received GenerateOk message"), 
            Payload::BroadcastOk { .. } => bail!("received BroadcastOk message"), 
            Payload::ReadOk { .. } => bail!("received ReadOk message"), 
            Payload::TopologyOk { .. } => bail!("received TopologyOk message"), 
        }

        Ok(())
    }
}
