use neo4rs::*;

mod container;

#[tokio::test]
pub(crate) async fn time_as_param() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/time_as_param.rs");
}
