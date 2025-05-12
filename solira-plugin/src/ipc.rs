use std::sync::Arc;

use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToNsName, tokio::prelude::*};
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, broadcast, mpsc},
    task::JoinHandle,
};

use crate::bridge::{Block, Transaction};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum SoliraMessage {
    Transaction(Transaction, u32),
    Block(Block),
    Exit,
}

pub type Tx = broadcast::Sender<SoliraMessage>;
pub type Rx = broadcast::Receiver<SoliraMessage>;
pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub async fn spawn_socket_server(
    mut rx: mpsc::UnboundedReceiver<SoliraMessage>,
) -> Result<JoinHandle<()>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let name = "solira.sock".to_ns_name::<GenericNamespaced>()?;
    let listener = ListenerOptions::new().name(name).create_tokio()?;

    // Shared list of live sockets
    let clients: Arc<Mutex<Vec<LocalSocketStream>>> = Arc::new(Mutex::new(Vec::new()));

    // Accept loop – adds sockets to the list
    {
        let clients = clients.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok(stream) => {
                        clients.lock().await.push(stream); // no `?`
                    }
                    Err(e) => log::error!("IPC accept error: {e}"),
                }
            }
        });
    }

    // Writer loop – one for all clients; slowest client back-pressures the producer
    Ok(tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let bytes = bincode::serialize(&msg).expect("serialize");
            let len = (bytes.len() as u32).to_le_bytes();

            let mut to_remove = Vec::new();

            let mut list = clients.lock().await; // await, no ?
            for (idx, stream) in list.iter_mut().enumerate() {
                if stream.write_all(&len).await.is_err() || stream.write_all(&bytes).await.is_err()
                {
                    to_remove.push(idx);
                }
            }
            for idx in to_remove.into_iter().rev() {
                list.remove(idx);
            }
        }
    }))
}
