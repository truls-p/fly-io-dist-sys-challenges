use fly::msg::{Message, EchoNode};
use anyhow::{Context};

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut stdout = std::io::stdout().lock();

    let mut state = EchoNode::new();

    for input in inputs {
        let input = input.context("Maelstrom input from STDIN could not be deserialized")?;
        state.step(input, &mut stdout).context("step failed")?;
    }

    Ok(())
}
