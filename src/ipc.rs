use interprocess::local_socket::{tokio::prelude::*, GenericNamespaced, ListenerOptions, ToNsName};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, sync::broadcast, task::JoinHandle};

use crate::bridge::{Block, Transaction};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum SoliraMessage {
    Transaction(Transaction),
    Block(Block),
}

pub type Tx = broadcast::Sender<SoliraMessage>;
pub type Rx = broadcast::Receiver<SoliraMessage>;
pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub async fn spawn_socket_server(tx: Tx) -> Result<JoinHandle<()>, Error> {
    let name = "solira.sock".to_ns_name::<GenericNamespaced>()?;

    let listener: LocalSocketListener = ListenerOptions::new().name(name).create_tokio()?; // async listener (sync = create_sync)

    Ok(tokio::spawn(async move {
        loop {
            let stream = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    log::error!("IPC accept error: {e}");
                    continue;
                }
            };
            tokio::spawn(handle_client(stream, tx.subscribe()));
        }
    }))
}

async fn handle_client(mut stream: LocalSocketStream, mut rx: Rx) {
    while let Ok(msg) = rx.recv().await {
        // Serialize with v1 API
        let bytes = bincode::serialize(&msg).expect("bincode serialize");

        // Length-prefix (u32 LE)
        let len = (bytes.len() as u32).to_le_bytes();
        if stream.write_all(&len).await.is_err() || stream.write_all(&bytes).await.is_err() {
            log::info!("IPC client disconnected");
            break;
        }
    }
}
