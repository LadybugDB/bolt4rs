use bolt4rs::*;

mod container;

// The purpose of the test is to not use a `must_use`
#[allow(unused_must_use)]
#[tokio::test]
async fn result_stream() {
    let bolt = container::BoltContainer::new().await;
    let graph = bolt.graph();

    include!("../include/result_stream.rs");
}
