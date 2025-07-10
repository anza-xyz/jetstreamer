use std::sync::Arc;

use crossbeam_channel::Receiver;
use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToNsName, tokio::prelude::*};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, sync::RwLock, task::JoinHandle};

use crate::bridge::{Block, Transaction};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum JetstreamerMessage {
    Transaction(Transaction, u32),
    Block(Block),
    Exit,
}

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub async fn spawn_socket_server(
    receiver: Receiver<JetstreamerMessage>,
    socket_id: usize,
) -> Result<JoinHandle<()>, Error> {
    let socket_name = format!("jetstreamer_{}.sock", socket_id);
    let name = socket_name.to_ns_name::<GenericNamespaced>()?;
    let listener = ListenerOptions::new().name(name).create_tokio()?;

    let clients: Arc<RwLock<Vec<LocalSocketStream>>> = Arc::new(RwLock::new(Vec::new()));

    {
        let clients = clients.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok(stream) => {
                        clients.write().await.push(stream);
                    }
                    Err(e) => log::error!("IPC accept error on socket {}: {e}", socket_id),
                }
            }
        });
    }

    Ok(tokio::spawn(async move {
        while let Ok(msg) = receiver.recv() {
            let bytes = bincode::serialize(&msg).expect("serialize");
            let len = (bytes.len() as u32).to_le_bytes();

            let mut to_remove = Vec::new();

            let mut list = clients.write().await;
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
