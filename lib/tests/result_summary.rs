#![cfg(feature = "unstable-result-summary")]
use neo4rs::*;

mod container;

#[tokio::test]
async fn streaming_summary() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/result_summary.rs");
}
