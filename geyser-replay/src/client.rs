pub enum ClientError {
    Network(reqwest::Error),
    NotFound,
}

pub struct CarClient {
    client: reqwest::Client,
}

impl CarClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub async fn get(&self, url: &str) -> Result<reqwest::Response, reqwest::Error> {
        self.client.get(url).send().await
    }
}
