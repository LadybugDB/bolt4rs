use bolt4rs::*;

mod container;

#[tokio::test]
async fn datetime_as_param() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/datetime_as_param.rs");
}
