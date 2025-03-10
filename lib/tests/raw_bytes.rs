use neo4rs::*;

mod container;

#[tokio::test]
async fn raw_bytes() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/raw_bytes.rs");
}
