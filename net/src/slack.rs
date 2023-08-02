use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tracing::instrument;

#[derive(Clone, Debug)]
pub struct SlackClient {
    url: Option<String>,
    timeout: Duration,
}

impl SlackClient {
    pub fn new(url: Option<String>, timeout: Duration) -> SlackClient {
        SlackClient { url, timeout }
    }

    #[instrument(skip(self))]
    pub async fn post_slack_message(&self, message: &str) -> bool {
        if let Some(url) = &self.url {
            let client = Client::new();
            let payload = json!({ "text": message });

            let response = client
                .post(url)
                .header("Content-Type", "application/json")
                .json(&payload)
                .timeout(self.timeout)
                .send()
                .await;
            if response.is_err() {
                false
            } else {
                response.unwrap().status().is_success()
            }
        } else {
            true
        }
    }
}
