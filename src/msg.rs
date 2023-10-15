
use serde::{Serialize, Deserialize};
use anyhow::{Context, bail};
use std::io::{StdoutLock, Write};


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
    Echo { echo: String },
    EchoOk { echo: String },
    Init { node_id: String, node_ids: Vec<String>, },
    InitOk,
}

pub struct EchoNode {
    pub id: usize,
}

impl EchoNode {
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
            Payload::EchoOk { .. } => {},
            Payload::InitOk{ .. } => bail!("received InitOk message"), 
        }

        Ok(())
    }
}
