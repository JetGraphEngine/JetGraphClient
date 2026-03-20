//! Health check client.

use tonic::transport::Channel;
use crate::ClientError;

pub mod health_proto {
    tonic::include_proto!("health");
}

use health_proto::health_service_client::HealthServiceClient;

#[derive(Clone)]
pub struct HealthClient {
    client: HealthServiceClient<Channel>,
}

impl HealthClient {
    pub(crate) fn new(channel: Channel) -> Self {
        Self {
            client: HealthServiceClient::new(channel),
        }
    }

    /// Check engine readiness.
    pub async fn check(&mut self) -> Result<bool, ClientError> {
        let r = self.client
            .check(health_proto::HealthRequest {})
            .await
            .map_err(ClientError::from)?;
        Ok(r.into_inner().status == health_proto::EngineStatus::Ready as i32)
    }
}
