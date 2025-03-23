use bolt4rs::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = "127.0.0.1:7687";
    let user = "bolt";
    let pass = "test";

    println!("Connecting to Bolt server at {}", uri);
    let graph = Graph::new(uri, user, pass)?;
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
    graph
        .run("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")
        .await?;
    println!("Query results:");
    println!("{:?}", ());

    Ok(())
}
