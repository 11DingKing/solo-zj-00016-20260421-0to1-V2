use chrono::{DateTime, Utc};
use redis::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::AppError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedShortLink {
    pub id: i32,
    pub short_code: String,
    pub original_url: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct CachePool {
    client: Arc<Client>,
}

impl CachePool {
    pub async fn new(redis_url: &str) -> Result<Self, AppError> {
        let client = Client::open(redis_url)?;
        let mut conn = client.get_async_connection().await?;
        redis::cmd("PING").query_async::<_, ()>(&mut conn).await?;
        
        Ok(Self {
            client: Arc::new(client),
        })
    }

    pub async fn get_url(&self, short_code: &str) -> Result<Option<String>, AppError> {
        let mut conn = self.client.get_async_connection().await?;
        let key = format!("url:{}", short_code);
        let result: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await?;
        
        Ok(result)
    }

    pub async fn set_url(
        &self,
        short_code: &str,
        original_url: &str,
        ttl_seconds: u64,
    ) -> Result<(), AppError> {
        let mut conn = self.client.get_async_connection().await?;
        let key = format!("url:{}", short_code);
        redis::cmd("SETEX")
            .arg(&key)
            .arg(ttl_seconds)
            .arg(original_url)
            .query_async::<_, ()>(&mut conn)
            .await?;
        
        Ok(())
    }

    pub async fn get_short_link(&self, short_code: &str) -> Result<Option<CachedShortLink>, AppError> {
        let mut conn = self.client.get_async_connection().await?;
        let key = format!("link:{}", short_code);
        let result: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await?;
        
        match result {
            Some(json) => {
                let link: CachedShortLink = serde_json::from_str(&json)?;
                Ok(Some(link))
            }
            None => Ok(None),
        }
    }

    pub async fn set_short_link(
        &self,
        link: &CachedShortLink,
        ttl_seconds: u64,
    ) -> Result<(), AppError> {
        let mut conn = self.client.get_async_connection().await?;
        let key = format!("link:{}", link.short_code);
        let json = serde_json::to_string(link)?;
        redis::cmd("SETEX")
            .arg(&key)
            .arg(ttl_seconds)
            .arg(json)
            .query_async::<_, ()>(&mut conn)
            .await?;
        
        Ok(())
    }

    pub async fn delete_short_link(&self, short_code: &str) -> Result<(), AppError> {
        let mut conn = self.client.get_async_connection().await?;
        let key = format!("link:{}", short_code);
        redis::cmd("DEL")
            .arg(&key)
            .query_async::<_, ()>(&mut conn)
            .await?;
        
        Ok(())
    }

    pub async fn increment_click_count(&self, short_link_id: i32) -> Result<(), AppError> {
        let mut conn = self.client.get_async_connection().await?;
        let key = format!("clicks:{}", short_link_id);
        redis::cmd("INCR")
            .arg(&key)
            .query_async::<_, ()>(&mut conn)
            .await?;
        
        Ok(())
    }

    pub async fn get_all_click_counts(&self) -> Result<HashMap<i32, i64>, AppError> {
        let mut conn = self.client.get_async_connection().await?;
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("clicks:*")
            .query_async(&mut conn)
            .await?;
        
        let mut counts = HashMap::new();
        for key in keys {
            let count: Option<i64> = redis::cmd("GET")
                .arg(&key)
                .query_async(&mut conn)
                .await?;
            
            if let Some(c) = count {
                if let Some(id_str) = key.strip_prefix("clicks:") {
                    if let Ok(id) = id_str.parse::<i32>() {
                        counts.insert(id, c);
                    }
                }
            }
        }
        
        Ok(counts)
    }

    pub async fn delete_click_counts(&self, ids: &[i32]) -> Result<(), AppError> {
        let mut conn = self.client.get_async_connection().await?;
        for id in ids {
            let key = format!("clicks:{}", id);
            redis::cmd("DEL")
                .arg(&key)
                .query_async::<_, ()>(&mut conn)
                .await?;
        }
        
        Ok(())
    }

    pub async fn get_rate_limit_count(&self, ip: &str) -> Result<u64, AppError> {
        let mut conn = self.client.get_async_connection().await?;
        let key = format!("ratelimit:{}", ip);
        let count: Option<u64> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await?;
        
        Ok(count.unwrap_or(0))
    }

    pub async fn increment_rate_limit(&self, ip: &str) -> Result<(), AppError> {
        let mut conn = self.client.get_async_connection().await?;
        let key = format!("ratelimit:{}", ip);
        
        let current: Option<u64> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await?;
        
        if current.is_none() {
            redis::cmd("SETEX")
                .arg(&key)
                .arg(60u64)
                .arg(1u64)
                .query_async::<_, ()>(&mut conn)
                .await?;
        } else {
            redis::cmd("INCR")
                .arg(&key)
                .query_async::<_, ()>(&mut conn)
                .await?;
        }
        
        Ok(())
    }
}
