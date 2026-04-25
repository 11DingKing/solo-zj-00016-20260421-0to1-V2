mod config;
mod error;
mod models;
mod db;
mod cache;
mod utils;
mod middleware;
mod handlers;
mod routes;

use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber;
use axum::{
    http::{
        header::{ACCEPT, CONTENT_TYPE, ORIGIN},
        HeaderValue, Method,
    },
    Router,
};
use tower_http::cors::{Any, CorsLayer};

use config::Config;
use db::DbPool;
use cache::CachePool;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::from_env();
    
    let db_pool = DbPool::new(&config.database_url).await
        .expect("Failed to create database pool");
    
    let cache_pool = CachePool::new(&config.redis_url).await
        .expect("Failed to create redis pool");

    let app_state = Arc::new(AppState {
        db: db_pool.clone(),
        cache: cache_pool.clone(),
        config: config.clone(),
    });

    let cors = CorsLayer::new()
        .allow_origin([
            "http://localhost:3000".parse::<HeaderValue>().unwrap(),
            "http://127.0.0.1:3000".parse::<HeaderValue>().unwrap(),
        ])
        .allow_headers([ACCEPT, CONTENT_TYPE, ORIGIN])
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
        .allow_credentials(true);

    let app = Router::new()
        .merge(routes::create_routes(app_state))
        .layer(cors);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!("Server listening on {}", addr);
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        loop {
            interval.tick().await;
            if let Err(e) = sync_click_counts(&db_pool, &cache_pool).await {
                tracing::error!("Failed to sync click counts: {}", e);
            }
        }
    });
    
    axum::Server::bind(&addr)
        .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .await
        .unwrap();
}

async fn sync_click_counts(db: &DbPool, cache: &CachePool) -> Result<(), Box<dyn std::error::Error>> {
    let counts = cache.get_all_click_counts().await?;
    
    if counts.is_empty() {
        return Ok(());
    }
    
    tracing::info!("Syncing {} click counts to database", counts.len());
    
    db.batch_update_click_counts(&counts).await?;
    
    let ids: Vec<i32> = counts.keys().cloned().collect();
    cache.delete_click_counts(&ids).await?;
    
    tracing::info!("Successfully synced {} click counts", ids.len());
    
    Ok(())
}

pub struct AppState {
    pub db: DbPool,
    pub cache: CachePool,
    pub config: Config,
}
