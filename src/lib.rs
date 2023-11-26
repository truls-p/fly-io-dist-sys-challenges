pub mod msg;





#[test]
fn func_test()  -> anyhow::Result<()> {

//{"id":5,"src":"c2","dest":"n0","body":{"type":"topology","topology":{"n0":["n1"],"n1":["n0"]},"msg_id":1}}
//{"id":12,"src":"c4","dest":"n0","body":{"type":"broadcast","message":1,"msg_id":2}}
    let init_msg: Message = serde_json::from_str(
        "{\"src\": \"c1\",\"dest\": \"n0\",\"body\": {\"type\":     \"init\", \"msg_id\":   1, \"node_id\":  \"n0\", \"node_ids\": [\"n1\"]}}"
    )
    .context("failed to deserialize init")?;

    let stdout = std::io::stdout().lock();
    let (tx, rx) = channel();
    let mut state = EchoNode::new(Event::Message(init_msg), stdout, tx.clone());

    //drop(stdin);
    //drop(stdin);
    /*
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
    */
    Ok(())
 }
    
