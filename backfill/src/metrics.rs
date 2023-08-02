use jito_net::slack::SlackClient;
use std::time::Instant;
use tracing::instrument;

pub struct BackfillMetrics {
    thread_id: usize,
    slot: u64,
    slack_client: SlackClient,

    created: Instant,
    last_update: Instant,
    num_messages_received: u64,
    num_messages_buffered: u64,
}

impl BackfillMetrics {
    const REPORT_PERIOD_S: u64 = 10 * 60;

    pub fn new(thread_id: usize, slot: u64, slack_client: SlackClient) -> BackfillMetrics {
        BackfillMetrics {
            thread_id,
            slot,
            slack_client,
            created: Instant::now(),
            last_update: Instant::now(),
            num_messages_received: 0,
            num_messages_buffered: 0,
        }
    }

    pub fn increment_num_messages_received(&mut self, count: u64) {
        self.num_messages_received += count;
    }

    pub fn increment_num_messages_buffered(&mut self, count: u64) {
        self.num_messages_buffered += count;
    }

    #[instrument(skip_all)]
    pub async fn report(&self) {
        let message = format!(
            "backfill metrics thread_id: {} slot: {}, elapsed: {:?}s num_messages_received: {} num_messages_buffered: {}",
            self.thread_id,
            self.slot,
            self.created.elapsed().as_secs(),
            self.num_messages_received,
            self.num_messages_buffered
        );
        self.slack_client.post_slack_message(&message).await;
    }

    #[instrument(skip_all)]
    pub async fn maybe_report(&mut self) {
        if self.last_update.elapsed().as_secs() > Self::REPORT_PERIOD_S {
            self.report().await;
            self.last_update = Instant::now();
        }
    }
}
