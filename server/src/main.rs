use anyhow::Result;
use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use bytes::{Bytes, BytesMut};
use lbug::{Connection, Database, QueryResult, SystemConfig};
use log::{debug, error};
use std::sync::Arc;
use std::{collections::HashMap, vec};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};
// Import with explicit feature flag
use bolt4rs::{
    bolt::{
        response::success,
        summary::{Failure, Success, Summary},
    },
    messages::{BoltRequest, BoltResponse, Record},
    BoltBoolean, BoltFloat, BoltInteger, BoltList, BoltNull, BoltString, BoltType,
};

const MAX_CHUNK_SIZE: usize = 65_535;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let system_db = Arc::new(lbug_init().await?);
    let listener = TcpListener::bind("0.0.0.0:7687").await?;
    debug!("Bolt server listening on 0.0.0.0:7687");

    loop {
        let (socket, addr) = listener.accept().await?;
        debug!("New connection from {}", addr);

        let db = system_db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(&db, socket).await {
                error!("Connection error: {}", e);
            }
        });
    }
}

async fn lbug_init() -> Result<lbug::Database> {
    let system_db = Database::new("./system", SystemConfig::default())?;
    debug!("Created system database");
    Ok(system_db)
}

async fn handle_connection(system_db: &Arc<lbug::Database>, socket: TcpStream) -> Result<()> {
    let mut stream = BufStream::new(socket);

    // Read first 4 bytes - these should be the magic bytes
    let mut magic = [0u8; 4];
    stream.read_exact(&mut magic).await?;
    debug!("Received magic bytes: {:02x?}", magic);

    if magic != [0x60, 0x60, 0xB0, 0x17] {
        return Err(anyhow::anyhow!("Invalid magic bytes"));
    }

    // Read supported versions (16 bytes)
    let mut versions = [0u8; 16];
    stream.read_exact(&mut versions).await?;
    debug!("Received version bytes: {:02x?}", versions);

    // Send back version 4.4
    let version = [0x00, 0x00, 0x04, 0x04]; // Changed to match expected format
    stream.write_all(&version).await?;
    stream.flush().await?;
    debug!("Handshake completed successfully");

    let mut session = BoltSession::new();

    loop {
        match read_chunked_message(&mut stream).await {
            Ok(Some(msg)) => {
                debug!("Received message bytes: {:02x?}", msg);
                let req = BoltRequest::parse(bolt4rs::Version::V4_1, msg)?;
                let responses = session.handle_message(system_db, req).await?;
                for response in responses {
                    debug!("Sending response bytes: {:02x?}", response);
                    send_chunked_message(&mut stream, response).await?;
                }
            }
            Ok(None) => {
                debug!("Received end of stream");
                break;
            }
            Err(e) => {
                error!("Error reading message: {}", e);
                break;
            }
        }
    }

    Ok(())
}

async fn read_chunked_message(stream: &mut BufStream<TcpStream>) -> Result<Option<Bytes>> {
    let mut message = BytesMut::new();

    loop {
        // Read 2-byte chunk size
        let mut size_bytes = [0u8; 2];
        match stream.read_exact(&mut size_bytes).await {
            Ok(_) => {
                let chunk_size = u16::from_be_bytes(size_bytes) as usize;
                debug!("Reading chunk of size {}", chunk_size);

                // Zero chunk size signals end of message
                if chunk_size == 0 {
                    return Ok(if message.is_empty() {
                        None
                    } else {
                        Some(message.freeze())
                    });
                }

                // Read chunk data
                let mut chunk = vec![0u8; chunk_size];
                stream.read_exact(&mut chunk).await?;
                message.extend_from_slice(&chunk);
            }
            Err(e) => {
                if message.is_empty() {
                    return Ok(None);
                } else {
                    return Err(e.into());
                }
            }
        }
    }
}

async fn send_chunked_message(stream: &mut BufStream<TcpStream>, message: Bytes) -> Result<()> {
    for chunk in message.chunks(MAX_CHUNK_SIZE) {
        let chunk_len = chunk.len() as u16;
        debug!("Sending chunk of size {}", chunk_len);
        stream.write_all(&chunk_len.to_be_bytes()).await?;
        stream.write_all(chunk).await?;
    }
    stream.write_all(&[0, 0]).await?; // End marker
    stream.flush().await?;
    Ok(())
}

#[derive(Debug)]
struct Query {
    statement: String,
    parameters: HashMap<String, String>,
}

struct BoltSession<'a> {
    authenticated: bool,
    current_query: Option<Query>,
    result: Option<QueryResult<'a>>,
    result_consumed: bool,
    has_more: bool,
}

// Return type changed to Vec<Bytes> to support multiple responses
impl<'a> BoltSession<'a> {
    fn new() -> Self {
        Self {
            authenticated: false,
            current_query: None,
            result: None,
            result_consumed: false,
            has_more: false,
        }
    }

    async fn get_col_info(&mut self) -> Result<Vec<String>> {
        Ok(self.result.as_ref().unwrap().get_column_names())
    }

    #[cfg_attr(feature = "unstable-bolt-protocol-impl-v2", allow(deprecated))]
    async fn handle_message(
        &mut self,
        db: &'a Arc<lbug::Database>,
        req: BoltRequest,
    ) -> Result<Vec<Bytes>> {
        debug!("Handling message: {req:?}");

        match req {
            // HELLO
            BoltRequest::Hello(_) => {
                self.authenticated = true;

                // Create success metadata with server info and connection id
                let metadata = success::MetaBuilder::new()
                    .server("lbug/0.12.0")
                    .connection_id("bolt-31") // Example connection ID
                    .build();

                // Create Success with metadata and wrap it in Summary
                let summary = Summary::Success(Success { metadata });

                Ok(vec![Bytes::from(summary.to_bytes()?)])
            }

            // RUN
            BoltRequest::Run(run) => {
                match self.authenticated {
                    false => Err(anyhow::anyhow!("Not authenticated")),
                    true => {
                        // Store the query
                        self.current_query = Some(Query {
                            statement: run.query.to_string(),
                            parameters: run
                                .parameters
                                .value
                                .into_iter()
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect(),
                        });

                        debug!("Stored query: {:?}", self.current_query);
                        let conn = Connection::new(db)?;

                        // Get the current query and substitute parameters
                        let final_query = match &self.current_query {
                            Some(Query {
                                statement,
                                parameters,
                            }) => {
                                let mut query_str = statement.clone();
                                for (key, value) in parameters {
                                    // Replace $key or {key} style parameters
                                    query_str = query_str
                                        .replace(&format!("${}", key), value)
                                        .replace(&format!("{{{}}}", key), value);
                                }
                                query_str
                            }
                            None => return Err(anyhow::anyhow!("No query stored")),
                        };

                        match conn.query(&final_query) {
                            Err(e) => {
                                error!("Query error: {}", e);
                                let summary = Summary::Failure(Failure {
                                    code: "Neo.ClientError.Statement.ExecutionError".to_string(),
                                    message: e.to_string(),
                                });
                                return Ok(vec![Bytes::from(summary.to_bytes()?)]);
                            }
                            Ok(result) => {
                                // Query executed successfully
                                debug!("Query executed successfully");
                                // For DDL operations like CREATE TABLE, mark as consumed immediately
                                let is_ddl =
                                    final_query.trim().to_uppercase().starts_with("CREATE ");
                                self.result = Some(result);
                                if is_ddl {
                                    self.result_consumed = true;
                                    self.has_more = false;
                                } else {
                                    self.result_consumed = false;
                                    self.has_more = true;
                                }
                            }
                        }
                        let cols = self.get_col_info().await?;
                        // Create success metadata with server info and connection id
                        let metadata = success::MetaBuilder::new()
                            .server("lbug/0.12.0")
                            .connection_id("bolt-31") // Example connection ID
                            .fields(cols.into_iter())
                            .build();

                        // Create Success with metadata and wrap it in Summary
                        let summary = Summary::Success(Success { metadata });

                        Ok(vec![Bytes::from(summary.to_bytes()?)])
                    }
                }
            }

            // PULL_ALL or PULL or PULL_N
            BoltRequest::Pull(pull) => {
                if self.result.is_none() {
                    return Err(anyhow::anyhow!("No results available"));
                }

                // For PULL_N, we need to read the n parameter
                let max_records = pull.extra.get("n").unwrap_or(1000) as usize; // Default to 1000 records for PULL/PULL_ALL
                let chunk_size = 100; // Process in smaller chunks

                let mut responses = Vec::new();
                let mut records_sent = 0;

                if !self.result_consumed {
                    // Get a mutable reference to the result
                    let result = self.result.as_mut().unwrap();
                    match result.iter_arrow(chunk_size) {
                        Ok(arrow_iter) => {
                            // Process batches
                            for batch in arrow_iter {
                                let num_columns = batch.num_columns();
                                let num_rows = batch.num_rows();

                                debug!("sending {} rows", num_rows);
                                for row_idx in 0..num_rows {
                                    if records_sent >= max_records {
                                        break;
                                    }

                                    // Create a Row for each record
                                    let mut values = vec![];
                                    for col_idx in 0..num_columns {
                                        let column = batch.column(col_idx);

                                        // Convert Arrow column to appropriate BoltType based on type
                                        let value = match column.data_type() {
                                            arrow::datatypes::DataType::Boolean => {
                                                let array = column
                                                    .as_any()
                                                    .downcast_ref::<BooleanArray>()
                                                    .unwrap();
                                                BoltType::Boolean(BoltBoolean {
                                                    value: array.value(row_idx),
                                                })
                                            }
                                            arrow::datatypes::DataType::Int64 => {
                                                let array = column
                                                    .as_any()
                                                    .downcast_ref::<Int64Array>()
                                                    .unwrap();
                                                BoltType::Integer(BoltInteger {
                                                    value: array.value(row_idx),
                                                })
                                            }
                                            arrow::datatypes::DataType::Float64 => {
                                                let array = column
                                                    .as_any()
                                                    .downcast_ref::<Float64Array>()
                                                    .unwrap();
                                                BoltType::Float(BoltFloat {
                                                    value: array.value(row_idx),
                                                })
                                            }
                                            arrow::datatypes::DataType::Utf8 => {
                                                let array = column
                                                    .as_any()
                                                    .downcast_ref::<StringArray>()
                                                    .unwrap();
                                                BoltType::String(BoltString {
                                                    value: array.value(row_idx).to_string(),
                                                })
                                            }
                                            _ => BoltType::Null(BoltNull {}), // Handle other types as null
                                        };
                                        values.push(value);
                                    }

                                    // Create values as a BoltList
                                    let data_values = BoltList { value: values };

                                    // Create a Record response for each row
                                    let record = Record { data: data_values };
                                    let response = BoltResponse::Record(record);
                                    let bytes = response.to_bytes()?;
                                    responses.push(bytes);
                                    records_sent += 1;
                                }
                                if records_sent >= max_records {
                                    break;
                                }
                            }

                            let exhausted_iter = records_sent < chunk_size;
                            self.result_consumed = records_sent >= max_records || exhausted_iter;
                            self.has_more = !self.result_consumed;

                            let metadata = success::MetaBuilder::new()
                                .server("lbug/0.12.0") // Example server version
                                .connection_id("bolt-31") // Example connection ID
                                .done(self.result_consumed) // Set done based on consumption
                                .has_more(!self.result_consumed && !exhausted_iter) // Set has_more based on consumption
                                .build();
                            // Create Success with metadata and wrap it in Summary
                            let summary = Summary::Success(Success { metadata });
                            responses.push(Bytes::from(summary.to_bytes()?));
                            Ok(responses)
                        }
                        Err(e) => {
                            error!("Error creating arrow iterator: {}", e);
                            Err(anyhow::anyhow!("Error creating arrow iterator"))
                        }
                    }
                } else {
                    let metadata = success::MetaBuilder::new()
                        .server("lbug/0.12.0") // Example server version
                        .connection_id("bolt-31") // Example connection ID
                        .build();
                    // Create Success with metadata and wrap it in Summary
                    let summary = Summary::Success(Success { metadata });
                    Ok(vec![Bytes::from(summary.to_bytes()?)])
                }
            }

            // Return empty success for unknown messages
            _ => {
                debug!("Unsupported message type: {:?}", req);
                let metadata = success::MetaBuilder::new()
                    .server("lbug/0.12.0") // Example server version
                    .connection_id("bolt-31") // Example connection ID
                    .build();
                // Create Success with metadata and wrap it in Summary
                let summary = Summary::Success(Success { metadata });
                Ok(vec![Bytes::from(summary.to_bytes()?)])
            }
        }
    }
}
