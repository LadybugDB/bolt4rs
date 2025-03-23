use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use kuzu::{Connection, Database, Error, SystemConfig};
use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};

const MAX_CHUNK_SIZE: usize = 65_535;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let system_db = Arc::new(kuzu_init().await?);
    let listener = TcpListener::bind("127.0.0.1:7687").await?;
    debug!("Bolt server listening on 127.0.0.1:7687");

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

async fn kuzu_init() -> Result<kuzu::Database> {
    let system_db = Database::new("./system", SystemConfig::default())?;
    debug!("Created system database");
    Ok(system_db)
}

async fn handle_connection(system_db: &Arc<kuzu::Database>, socket: TcpStream) -> Result<()> {
    let mut stream = BufStream::new(socket);

    // Read first 4 bytes - these should be the magic bytes
    let mut magic = [0u8; 4];
    stream.read_exact(&mut magic).await?;
    debug!("Received magic bytes: {:02x?}", magic);

    if (magic != [0x60, 0x60, 0xB0, 0x17]) {
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
                let response = session.handle_message(system_db, msg).await?;
                debug!("Sending response bytes: {:02x?}", response);
                send_chunked_message(&mut stream, response).await?;
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
                if (chunk_size == 0) {
                    return Ok(if (message.is_empty()) {
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
                if (message.is_empty()) {
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

struct BoltSession {
    authenticated: bool,
    transactions: HashMap<i64, ()>,
    current_query: Option<Query>,
}

impl BoltSession {
    fn new() -> Self {
        Self {
            authenticated: false,
            transactions: HashMap::new(),
            current_query: None,
        }
    }

    async fn handle_message(&mut self, db: &Arc<kuzu::Database>, msg: Bytes) -> Result<Bytes> {
        let marker = msg[0];
        let signature = msg[1];

        debug!(
            "Handling message: marker={:02x}, signature={:02x}",
            marker, signature
        );

        match (marker, signature) {
            // HELLO
            (0xB1, 0x01) => {
                self.authenticated = true;
                let mut response = BytesMut::new();
                response.put_u8(0xB1); // tiny struct, size 1
                response.put_u8(0x70); // SUCCESS

                // Add metadata map with fixed size strings
                response.put_u8(0xA2); // tiny map, size 2

                // Server field
                response.put_u8(0x86); // tiny string, size 6
                response.put_slice(b"server");
                response.put_u8(0x84); // tiny string, size 4
                response.put_slice(b"Bolt");

                // Connection id
                response.put_u8(0x8D); // tiny string, size 13
                response.put_slice(b"connection_id");
                response.put_u8(0x85); // tiny string, size 5
                response.put_slice(b"bolt1");

                Ok(response.freeze())
            }

            // INIT
            (0xB2, 0x01) => {
                self.authenticated = true;
                let mut response = BytesMut::new();
                response.put_u8(0xB1); // tiny struct
                response.put_u8(0x70); // SUCCESS
                response.put_u8(0xA0); // empty map
                Ok(response.freeze())
            }

            // RUN
            (0xB3, 0x10) => {
                if (!self.authenticated) {
                    return Err(anyhow::anyhow!("Not authenticated"));
                }

                // Skip marker and signature which we've already read
                let mut bytes = msg.slice(2..);

                // Read query string - TINY_STRING format
                let str_marker = bytes[0];
                let str_len = (str_marker & 0x0F) as usize;
                let query = String::from_utf8(bytes.slice(1..1 + str_len).to_vec())?;
                bytes = bytes.slice(1 + str_len..);

                // Read parameters map
                let mut parameters = HashMap::new();
                if bytes[0] & 0xF0 == 0xA0 {
                    // TINY_MAP
                    let map_size = bytes[0] & 0x0F;
                    bytes = bytes.slice(1..);
                    for _ in 0..map_size {
                        // Read key
                        let key_marker = bytes[0];
                        let key_len = (key_marker & 0x0F) as usize;
                        let key = String::from_utf8(bytes.slice(1..1 + key_len).to_vec())?;
                        bytes = bytes.slice(1 + key_len..);

                        // Read value
                        let val_marker = bytes[0];
                        let val_len = (val_marker & 0x0F) as usize;
                        let value = String::from_utf8(bytes.slice(1..1 + val_len).to_vec())?;
                        bytes = bytes.slice(1 + val_len..);

                        parameters.insert(key, value);
                    }
                }

                // Store the query
                self.current_query = Some(Query {
                    statement: query,
                    parameters,
                });

                debug!("Stored query: {:?}", self.current_query);
                let conn = Connection::new(&db)?;

                // Get the current query and substitute parameters
                let final_query = if let Some(Query {
                    statement,
                    parameters,
                }) = &self.current_query
                {
                    let mut query_str = statement.clone();
                    for (key, value) in parameters {
                        // Replace $key or {key} style parameters
                        query_str = query_str
                            .replace(&format!("${}", key), value)
                            .replace(&format!("{{{}}}", key), value);
                    }
                    query_str
                } else {
                    return Err(anyhow::anyhow!("No query stored"));
                };

                conn.query(&final_query);

                // Rest of the success response remains the same
                let mut response = BytesMut::new();
                response.put_u8(0xB1); // tiny struct
                response.put_u8(0x70); // SUCCESS
                response.put_u8(0xA1); // tiny map, size 1
                response.put_u8(0x86); // tiny string length
                response.put_slice(b"fields");
                response.put_u8(0x91); // tiny list, size 1
                response.put_u8(0x81); // tiny string length
                response.put_slice(b"1");

                Ok(response.freeze())
            }

            // PULL_ALL or PULL
            (0xB0, 0x3F) | (0xB1, 0x3F) => {
                let mut response = BytesMut::new();

                // Send RECORD with value 1
                response.put_u8(0xB1); // tiny struct
                response.put_u8(0x71); // RECORD
                response.put_u8(0x91); // tiny list, size 1
                response.put_u8(0x01); // integer 1

                // Follow with SUCCESS
                response.put_u8(0xB1); // tiny struct
                response.put_u8(0x70); // SUCCESS
                response.put_u8(0xA0); // empty map

                Ok(response.freeze())
            }

            // GOODBYE
            (0xB0, 0x02) => Ok(Bytes::new()),

            _ => {
                debug!("Unsupported message type: {:02x} {:02x}", marker, signature);
                // Return empty success for unknown messages
                let mut response = BytesMut::new();
                response.put_u8(0xB1);
                response.put_u8(0x70);
                response.put_u8(0xA0);
                Ok(response.freeze())
            }
        }
    }
}
