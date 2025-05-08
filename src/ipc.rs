use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions;
use interprocess::local_socket::{tokio::prelude::*, GenericNamespaced, ListenerOptions, ToNsName};
use serde::Serialize;
use tokio::{io::AsyncWriteExt, sync::broadcast, task::JoinHandle};

#[derive(Serialize, Clone)]
pub enum SoliraMessage {
    Transaction { slot: u64 },
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
        if let Ok(json) = serde_json::to_string(&msg) {
            if stream.write_all(json.as_bytes()).await.is_err()
                || stream.write_all(b"\n").await.is_err()
            {
                log::info!("IPC client disconnected");
                break;
            }
        }
    }
}
