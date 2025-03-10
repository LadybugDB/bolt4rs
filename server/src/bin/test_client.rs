use neo4rs::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = "127.0.0.1:7687";
    let user = "bolt";
    let pass = "test";

    println!("Connecting to Bolt server at {}", uri);

    let graph = Graph::new(uri, user, pass)?;

    println!("Successfully connected!");

    println!("Running test query...");
    let result = graph.run("RETURN 1").await?;
    println!("Query executed successfully");

    Ok(())
}
