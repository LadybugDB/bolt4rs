# Bolt4rs [![CI Status][ci-badge]][ci-url]

[ci-badge]: https://github.com/LadybugDB/bolt4rs/actions/workflows/checks.yml/badge.svg
[ci-url]: https://github.com/LadybugDB/bolt4rs

`bolt4rs` is a driver for the [LadybugDB](https://ladybugdb.com/) graph database, written in Rust.
It's based on the neo4rs [implementation](https://github.com/neo4j-labs/neo4rs).

`bolt4rs` implements the [bolt specification](https://neo4j.com/docs/bolt/current/bolt/message/#messages-summary-41)

## Testing Ladybug + Bolt4rs

```
./server/test.sh
Cleaning up any existing bolt4rs-server process...
Starting bolt4rs-server...
Running test client...
Connecting to Bolt server at 127.0.0.1:7687
Successfully connected!
Creating Person table...
Inserting sample data...
Querying data...
Query results:
Name: Alice, Age: 25
Name: Bob, Age: 30
Total rows returned: 2
Test succeeded!
Cleaning up...
```

## API Documentation:

## Example

```rust
    // concurrent queries
    let uri = "127.0.0.1:7687";
    let user = "bolt";
    let pass = "neo";
    let graph = Graph::new(&uri, user, pass).unwrap();
    for _ in 1..=42 {
        let graph = graph.clone();
        tokio::spawn(async move {
            let mut result = graph.execute(
               query("MATCH (p:Person {name: $name}) RETURN p").param("name", "Mark")
            ).await.unwrap();
            while let Some(row) = result.next().await.unwrap() {
                let node: Node = row.get("p").unwrap();
                let name: String = node.get("name").unwrap();
                println!("{}", name);
            }
        });
    }

    //Transactions
    let mut txn = graph.start_txn().await.unwrap();
    txn.run_queries([
        "CREATE (p:Person {name: 'mark'})",
        "CREATE (p:Person {name: 'jake'})",
        "CREATE (p:Person {name: 'luke'})",
    ])
    .await
    .unwrap();
    txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();
```

## MSRV

The crate has a minimum supported Rust version (MSRV) of `1.75.0` as of 0.9.x.
The version [0.8.x](https://crates.io/crates/bolt4rs/0.8.0) has an MSRV of `1.63.0`

A change in the MSRV in *not* considered a breaking change.
For versions past 1.0.0, a change in the MSRV can be done in a minor version increment (1.1.3 -> 1.2.0)
for versions before 1.0.0, a change in the MSRV can be done in a patch version increment (0.1.3 -> 0.1.4).

## Implementation progress

> [!IMPORTANT]
> This driver is a work in progress, and not all features are implemented yet.

Only Bolt protocol versions 4.0 and 4.1 are supported.
Support for later versions is planned and in progress.

This means, that certain features like bookmarks or element IDs are not supported yet.

## Development

### Testing

This crate contains unit tests and integration tests.
The unit tests are run with `cargo test --lib` and do not require a running Bolt instance.
The integration tests are run with `cargo test` and require a running Bolt instance.

#### Running the integration tests

To run the tests, you need to have either docker or an existing Bolt instance running.
Docker is recommended since the tests don't necessarily clean up after themselves.

##### Using Docker

To run the tests with docker, you need to have docker installed and running.
You can control the version of Bolt that is used by setting the `BOLT_VERSION_TAG` environment variable.
The default version is `4.2`.
The tests will use the official `bolt` docker image, with the provided version as tag.

You might run into panics or test failures with the message 'failed to start container'.
In that case, try to pull the image first before running the tests with `docker pull bolt:$BOLT_VERSION_TAG`.

This could happen if you are on a machine with an architecture that is not supported by the image, e.g. `arm64` like the Apple silicon Macs.
In that case, pulling the image will fail with a message like 'no matching manifest for linux/arm64/v8'.
You need to use the `--platform` flag to pull the image for a different architecture, e.g. `docker pull --platform linux/amd64 bolt:$BOLT_VERSION_TAG`.
There is an experimental option in docker to use Rosetta to run those images, so that tests don't take forever to run (please check the docker documentation).

You could also use a newer bolt version like `4.4` instead, which has support for `arm64` architecture.

##### Using an existing Bolt instance

To run the tests with an existing Bolt instance, you need to have the `BOLT_TEST_URI` environment variable set to the connection string, e.g. `bolt+s://42421337thisisnotarealinstance.databases.bolt.io`.
The default user is `bolt`, but it can be changed with the `BOLT_TEST_USER` environment variable.
The default password is `neo`, but it can be changed with the `BOLT_TEST_PASS` environment variable.

Some tests might run different queries depending on the Bolt version.
You can use the `BOLT_VERSION_TAG` environment variable to set the version of the Bolt instance.

It is recommended to only run a single integration test and manually clean up the database after the test.

```sh
env BOLT_TEST_URI=bolt://localhost:7687 BOLT_TEST_USER=bolt BOLT_TEST_PASS=supersecret BOLT_VERSION_TAG=5.8 cargo test --test <name of the integration test, see the file names in lib/tests/>
```
### Updating `Cargo.lock` files for CI

We have CI tests that verify the MSRV as well as the minimal version of the dependencies.
The minimal versions are the lowest version that still satisfies the `Cargo.toml` entries, instead of the default of the highest version.

If you change anything in the `Cargo.toml`, you need to update the `Cargo.lock` files for CI.

This project uses [xtask](https://github.com/matklad/cargo-xtask#cargo-xtask)s to help with updating the lock files.

It is recommended to close all editors, or more specifically, all rust-analyzer instances for this project before running the commands below.

#### Update `ci/Cargo.lock.msrv`

```bash
# If there are errors, update Cargo.toml to fix and try again from the top.
# You might have to downgrade or remove certain crates to hit the MSRV.
# A number of such downgrades are already defined in the `update_msrv_lock` function
# in the xtask script.
# Alternatively, you might suggest an increase of the MSRV.
cargo xtask msrv
```

Using `xtask` requires that `curl` and `jq` are available on the system.


#### Update `ci/Cargo.lock.min`

```bash
# If there are errors, update Cargo.toml to fix and try again from the top.
cargo xtask min
```

Using `xtask` requires that `curl` and `jq` are available on the system.


## License

Bolt4rs is licensed under either of the following, at your option:

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
