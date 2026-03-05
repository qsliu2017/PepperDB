use std::sync::Arc;

use tokio::net::TcpListener;

use pepper_db::server::PepperServerFactory;
use pepper_db::Database;

#[tokio::main]
async fn main() {
    let data_dir = std::path::Path::new("pepper_data");
    let db = Arc::new(Database::new(data_dir));
    let factory = Arc::new(PepperServerFactory::new(db));

    let listener = TcpListener::bind("127.0.0.1:5432").await.unwrap();
    println!("PepperDB listening on 127.0.0.1:5432");

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        println!("New connection from {}", addr);
        let factory_ref = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = pgwire::tokio::process_socket(socket, None, factory_ref).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}
