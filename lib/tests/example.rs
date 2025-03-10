use neo4rs::*;

mod container;

#[tokio::test]
async fn basic_example() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/example.rs");
}
