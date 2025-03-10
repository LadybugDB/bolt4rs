use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use log::{error, info};
use std::collections::HashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};

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

async fn handle_connection(socket: TcpStream) -> Result<()> {
    let mut stream = BufStream::new(socket);

    // Read first 4 bytes - these should be the magic bytes
    let mut magic = [0u8; 4];
    stream.read_exact(&mut magic).await?;
    info!("Received magic bytes: {:02x?}", magic);

    if (magic != [0x60, 0x60, 0xB0, 0x17]) {
        return Err(anyhow::anyhow!("Invalid magic bytes"));
    }

    // Read supported versions (16 bytes)
    let mut versions = [0u8; 16];
    stream.read_exact(&mut versions).await?;
    info!("Received version bytes: {:02x?}", versions);

    // Send back version 4.4
    let version = [0x00, 0x00, 0x04, 0x04]; // Changed to match expected format
    stream.write_all(&version).await?;
    stream.flush().await?;
    info!("Handshake completed successfully");

    let mut session = BoltSession::new();

    loop {
        match read_chunked_message(&mut stream).await {
            Ok(Some(msg)) => {
                info!("Received message bytes: {:02x?}", msg);
                let response = session.handle_message(msg).await?;
                info!("Sending response bytes: {:02x?}", response);
                send_chunked_message(&mut stream, response).await?;
            }
            Ok(None) => {
                info!("Received end of stream");
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
                info!("Reading chunk of size {}", chunk_size);

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
        info!("Sending chunk of size {}", chunk_len);
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

    async fn handle_message(&mut self, msg: Bytes) -> Result<Bytes> {
        let marker = msg[0];
        let signature = msg[1];

        info!(
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
                response.put_u8(0x85); // tiny string, size 5
                response.put_slice(b"Neo4j");

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

                // Success response with fields
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
                info!("Unsupported message type: {:02x} {:02x}", marker, signature);
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
