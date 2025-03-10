use bolt4rs::*;

mod container;

#[tokio::test]
async fn transactions() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/transactions.rs");
}
