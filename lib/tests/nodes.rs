use bolt4rs::*;

mod container;

#[tokio::test]
async fn nodes() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/nodes.rs");
}
