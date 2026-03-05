use std::sync::Arc;

use tokio::net::TcpListener;

use pepper_db::server::PepperServerFactory;

#[tokio::main]
async fn main() {
    let factory = Arc::new(PepperServerFactory::new());

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
