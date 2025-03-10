use neo4rs::*;

mod container;

#[tokio::test]
async fn streams_within_a_transaction() {
    let config = ConfigBuilder::default().fetch_size(1);
    let bolt = container::BoltContainer::from_config(config).await;
    let graph = bolt.graph();

    include!("../include/streams_within_a_transaction.rs");
}
