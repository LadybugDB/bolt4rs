use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{error, info};
use std::collections::HashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};

const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const MAX_CHUNK_SIZE: usize = 65_535;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let listener = TcpListener::bind("127.0.0.1:7687").await?;
    info!("Bolt server listening on 127.0.0.1:7687");

    loop {
        let (socket, addr) = listener.accept().await?;
        info!("New connection from {}", addr);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                error!("Connection error: {}", e);
            }
        });
    }
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

    async fn handle_message(&mut self, msg: Bytes) -> Result<Bytes> {
        let marker = msg[0];
        let signature = msg[1];

        match (marker, signature) {
            // HELLO
            (0xB1, 0x01) => {
                self.authenticated = true;
                // Success response with server info
                let mut response = BytesMut::new();
                response.put_u8(0xB1); // tiny struct
                response.put_u8(0x70); // SUCCESS

                // Add server info map
                response.put_u8(0xA3); // tiny map, size 3

                // Server field
                response.put_u8(0x86); // tiny string length
                response.put_slice(b"server");
                response.put_u8(0x88); // tiny string length
                response.put_slice(b"Neo4j/4.1");

                // Connection ID
                response.put_u8(0x8C); // tiny string length
                response.put_slice(b"connection_id");
                response.put_u8(0x85); // tiny string length
                response.put_slice(b"bolt-1");

                // Protocol version
                response.put_u8(0x89); // tiny string length
                response.put_slice(b"protocol");
                response.put_u8(0x84); // tiny string length
                response.put_slice(b"4.1.0");

                Ok(response.freeze())
            }

            // RUN
            (0xB3, 0x10) => {
                if (!self.authenticated) {
                    return Err(anyhow::anyhow!("Not authenticated"));
                }

                // Parse query from message (simplified)
                self.current_query = Some(Query {
                    statement: "RETURN 1".to_string(),
                    parameters: HashMap::new(),
                });

                // Success response with query info
                let mut response = BytesMut::new();
                response.put_u8(0xB1);
                response.put_u8(0x70);
                response.put_u8(0xA1); // tiny map, size 1

                // Fields info
                response.put_u8(0x86);
                response.put_slice(b"fields");
                response.put_u8(0x91); // tiny list, size 1
                response.put_u8(0x81);
                response.put_slice(b"1");

                Ok(response.freeze())
            }

            // PULL
            (0xB1, 0x3F) => {
                if let Some(query) = &self.current_query {
                    // Send a record with the result
                    let mut response = BytesMut::new();
                    response.put_u8(0xB1);
                    response.put_u8(0x71); // RECORD
                    response.put_u8(0x91); // tiny list, size 1
                    response.put_u8(0x01); // integer 1

                    // Follow with SUCCESS
                    response.put_u8(0xB1);
                    response.put_u8(0x70);
                    response.put_u8(0xA0); // empty map

                    self.current_query = None;
                    Ok(response.freeze())
                } else {
                    let mut response = BytesMut::new();
                    response.put_u8(0xB1);
                    response.put_u8(0x70);
                    response.put_u8(0xA0);
                    Ok(response.freeze())
                }
            }

            // GOODBYE
            (0xB0, 0x02) => Ok(Bytes::new()),

            _ => Err(anyhow::anyhow!(
                "Unsupported message type: {:02x} {:02x}",
                marker,
                signature
            )),
        }
    }
}

async fn handle_connection(socket: TcpStream) -> Result<()> {
    let mut stream = BufStream::new(socket);

    // Read magic bytes
    let mut magic = [0u8; 4];
    stream.read_exact(&mut magic).await?;

    if magic != BOLT_MAGIC {
        return Err(anyhow::anyhow!("Invalid Bolt magic bytes"));
    }

    // Send version 4.1 in correct format [0, 0, 1, 4]
    stream.write_all(&[0, 0, 1, 4]).await?;
    stream.flush().await?;

    let mut session = BoltSession::new();

    loop {
        match read_message(&mut stream).await {
            Ok(Some(msg)) => {
                let response = session.handle_message(msg).await?;
                send_message(&mut stream, response).await?;
            }
            Ok(None) => break,
            Err(e) => {
                error!("Error reading message: {}", e);
                break;
            }
        }
    }

    Ok(())
}

async fn read_message(stream: &mut BufStream<TcpStream>) -> Result<Option<Bytes>> {
    let mut chunk_size = stream.read_u16().await? as usize;
    if chunk_size == 0 {
        return Ok(None);
    }

    let mut message = BytesMut::new();
    while chunk_size > 0 {
        let mut chunk = vec![0; chunk_size];
        stream.read_exact(&mut chunk).await?;
        message.extend_from_slice(&chunk);

        chunk_size = stream.read_u16().await? as usize;
    }

    Ok(Some(message.freeze()))
}

async fn send_message(stream: &mut BufStream<TcpStream>, message: Bytes) -> Result<()> {
    for chunk in message.chunks(MAX_CHUNK_SIZE) {
        stream.write_u16(chunk.len() as u16).await?;
        stream.write_all(chunk).await?;
    }
    stream.write_u16(0).await?; // End marker
    stream.flush().await?;
    Ok(())
}
