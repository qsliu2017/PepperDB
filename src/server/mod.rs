use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, PgWireHandlerFactory};
use pgwire::error::PgWireResult;

use crate::executor;
use crate::parser;

pub struct PepperHandler;

#[async_trait]
impl SimpleQueryHandler for PepperHandler {
    async fn do_query<'a, C: ClientInfo + Unpin + Send + Sync>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>> {
        let statements = parser::parse(query)?;
        let mut responses = Vec::new();
        for stmt in statements {
            responses.push(executor::execute(stmt)?);
        }
        Ok(responses)
    }
}

pub struct PepperServerFactory {
    handler: Arc<PepperHandler>,
}

impl PepperServerFactory {
    pub fn new() -> Self {
        Self {
            handler: Arc::new(PepperHandler),
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
