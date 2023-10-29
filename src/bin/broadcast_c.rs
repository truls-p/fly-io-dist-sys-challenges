use anyhow::Context;
use fly::msg::{EchoNode, Message};
use log::info;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    info!("Setting up STDIN/STDOUT");
    let stdin = std::io::stdin().lock();
    let mut inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let stdout = std::io::stdout().lock();

    let init_msg = inputs.next().context("expected Init message").unwrap()?;

    info!("Creating node");
    let mut state = EchoNode::new(init_msg, stdout);

    info!("Deserialising messages");
    for input in inputs {
        let input = input.context("Maelstrom input from STDIN could not be deserialized")?;
        state.step(input).context("step failed")?;
    }

    Ok(())
}
