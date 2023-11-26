use anyhow::Context;
use fly::msg::{EchoNode, Event, Message};
use log::{debug, info};
use std::io::BufRead;
use std::sync::mpsc::channel;
use std::thread;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    info!("Setting up STDIN/STDOUT");
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();

    let stdout = std::io::stdout().lock();

    let (tx, rx) = channel();

    debug!("reading init message");

    let init_msg: Message = serde_json::from_str(
        &stdin
            .next()
            .expect("valid message")
            .context("failed to read init message")?,
    )
    .context("failed to deserialize init")?;

    info!("Creating node");
    let mut state = EchoNode::new(Event::Message(init_msg), stdout, tx.clone());

    drop(stdin);
    //drop(stdin);

    let jh = thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let line = line.context("Malestrom line from STDIN not read")?;
            debug!("{:?}", &line);
            let input: Message = serde_json::from_str(&line).context("could not deserialize")?;
            if let Err(_) = tx.send(Event::Message(input)) {
                return Ok::<_, anyhow::Error>(());
            }
        }
        let _ = tx.send(Event::EOF);
        Ok(())
    });

    info!("Deserialising messages");
    for input in rx {
        state.step(input).context("step failed")?;
    }

    let _ = jh.join().expect("jh expect");

    Ok(())
}
