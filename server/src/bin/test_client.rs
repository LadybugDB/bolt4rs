use bolt4rs::*;
use clap::Parser;
use std::error::Error;

#[derive(Parser)]
#[command(name = "test_client")]
#[command(about = "Test client for Bolt4rs")]
struct Args {
    /// The URI of the Bolt server
    #[arg(default_value = "127.0.0.1:7687")]
    uri: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let uri = args.uri;
    let user = "bolt";
    let pass = "test";

    env_logger::init();
    println!("Connecting to Bolt server at {}", uri);
    let graph = Graph::new(&uri, user, pass)?;
    println!("Successfully connected!");

    println!("Creating Person table...");
    graph
        .run("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")
        .await?;

    println!("Inserting sample data...");
    graph
        .run("CREATE (:Person {name: 'Alice', age: 25});")
        .await?;
    graph
        .run("CREATE (:Person {name: 'Bob', age: 30});")
        .await?;

    println!("Querying data...");
    let mut stream = graph
        .execute("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")
        .await?;

    println!("Query results:");
    let mut count = 0;
    while let Ok(Some(row)) = stream.next().await {
        println!(
            "Name: {}, Age: {}",
            row.get::<String>("NAME")?,
            row.get::<i64>("AGE")?
        );
        count += 1;
    }
    println!("Total rows returned: {}", count);

    // Finish the stream and get summary
    stream.finish().await?;

    Ok(())
}
