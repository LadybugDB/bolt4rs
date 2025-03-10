use neo4rs::*;

mod container;

#[tokio::test]
async fn durations() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/durations.rs");
}
