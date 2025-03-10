use bolt4rs::*;

mod container;

#[tokio::test]
pub async fn path() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/path.rs");
}
