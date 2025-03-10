use bolt4rs::*;

mod container;

#[tokio::test]
async fn unbounded_relationships() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/unbounded_relationships.rs");
}
