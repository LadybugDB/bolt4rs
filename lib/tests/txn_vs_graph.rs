use bolt4rs::*;

mod container;

#[tokio::test]
async fn txn_vs_graph() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/txn_vs_graph.rs");
}
