use neo4rs::*;

mod container;

#[tokio::test]
async fn configurations() {
    let config = ConfigBuilder::default()
        .db("bolt")
        .fetch_size(500)
        .max_connections(10);
    let bolt = container::BoltContainer::from_config(config).await;
    let graph = bolt.graph();

    include!("../include/configurations.rs");
}
