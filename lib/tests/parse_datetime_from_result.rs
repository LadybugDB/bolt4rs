use neo4rs::*;

mod container;

#[tokio::test]
async fn parse_datetime_from_result() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/parse_datetime_from_result.rs");
}
