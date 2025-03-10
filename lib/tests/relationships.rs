use bolt4rs::*;

mod container;

#[tokio::test]
async fn relationships() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/relationships.rs");
}
