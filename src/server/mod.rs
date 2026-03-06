//! pgwire server handler: bridges network protocol to executor.

use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, PgWireHandlerFactory};
use pgwire::error::PgWireResult;

use crate::Database;

pub struct PepperHandler {
    db: Arc<Database>,
}

#[async_trait]
impl SimpleQueryHandler for PepperHandler {
    async fn do_query<'a, C: ClientInfo + Unpin + Send + Sync>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>> {
        let mut responses = Vec::new();
        for stmt_sql in query.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            let sql = format!("{};", stmt_sql);
            responses.push(self.db.execute_sql(&sql).await?);
        }
        Ok(responses)
    }
}

pub struct PepperServerFactory {
    handler: Arc<PepperHandler>,
}

impl PepperServerFactory {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            handler: Arc::new(PepperHandler { db }),
        }
    }
}

impl PgWireHandlerFactory for PepperServerFactory {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = PepperHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = pgwire::api::copy::NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(pgwire::api::copy::NoopCopyHandler)
    }
}
