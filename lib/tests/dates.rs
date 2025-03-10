use neo4rs::*;

mod container;

#[tokio::test]
async fn dates() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/dates.rs");
}
