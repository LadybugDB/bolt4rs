use bolt4rs::*;

mod container;

#[tokio::test]
async fn rollback_a_transaction() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/rollback_a_transaction.rs");
}
