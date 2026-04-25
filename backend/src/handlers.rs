use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Redirect, Response},
    Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

use crate::AppState;
use crate::cache::CachedShortLink;
use crate::error::AppError;
use crate::middleware::UserCookie;
use crate::models::{
    CreateShortLinkRequest, PaginatedUserLinksResponse, ShortLinkResponse, StatsResponse,
    UserLinksResponse,
};
use crate::utils::{generate_short_code, validate_url};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaginationQuery {
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

pub async fn create_short_link(
    State(state): State<Arc<AppState>>,
    user_cookie: UserCookie,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(payload): Json<CreateShortLinkRequest>,
) -> Result<Json<ShortLinkResponse>, AppError> {
    let ip = headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            headers.get("x-real-ip")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| addr.ip().to_string());

    let count = state.cache.get_rate_limit_count(&ip).await?;
    if count >= state.config.rate_limit_per_minute {
        return Err(AppError::RateLimitExceeded);
    }
    state.cache.increment_rate_limit(&ip).await?;

    if !validate_url(&payload.original_url) {
        return Err(AppError::InvalidUrl);
    }

    let mut short_code = generate_short_code(6);
    let mut attempts = 0;
    
    while attempts < 5 {
        let existing = state.db.get_short_link(&short_code).await?;
        if existing.is_none() {
            break;
        }
        short_code = generate_short_code(6);
        attempts += 1;
    }

    if attempts >= 5 {
        return Err(AppError::Internal);
    }

    let link = state
        .db
        .create_short_link(
            &short_code,
            &payload.original_url,
            &user_cookie.0,
            payload.expires_in_hours,
            payload.expires_at,
        )
        .await?;

    let cached_link = CachedShortLink {
        id: link.id,
        short_code: link.short_code.clone(),
        original_url: link.original_url.clone(),
        expires_at: link.expires_at,
    };
    state.cache.set_short_link(&cached_link, state.config.cache_ttl_seconds).await?;

    let host = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost:8080");
    
    let scheme = if host.contains("localhost") { "http" } else { "https" };
    let short_url = format!("{}://{}/{}", scheme, host, short_code);

    let is_expired = link.expires_at.map(|e| Utc::now() > e).unwrap_or(false);

    Ok(Json(ShortLinkResponse {
        short_code: link.short_code,
        original_url: link.original_url,
        short_url,
        created_at: link.created_at,
        expires_at: link.expires_at,
        total_clicks: 0,
        is_expired,
    }))
}

pub async fn get_user_links(
    State(state): State<Arc<AppState>>,
    user_cookie: UserCookie,
    headers: HeaderMap,
    Query(query): Query<PaginationQuery>,
) -> Result<Response, AppError> {
    let host = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost:8080");
    
    let scheme = if host.contains("localhost") { "http" } else { "https" };
    let base_url = format!("{}://{}", scheme, host);

    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).clamp(1, 100);

    if query.page.is_some() || query.page_size.is_some() {
        let (links, total) = state.db.get_user_links_paginated(&user_cookie.0, &base_url, page, page_size).await?;
        let total_pages = ((total as f64) / (page_size as f64)).ceil() as i64;
        
        Ok(Json(PaginatedUserLinksResponse {
            links,
            total,
            page,
            page_size,
            total_pages,
        }).into_response())
    } else {
        let links = state.db.get_user_links(&user_cookie.0, &base_url).await?;
        Ok(Json(UserLinksResponse { links }).into_response())
    }
}

pub async fn get_link_stats(
    State(state): State<Arc<AppState>>,
    Path(short_code): Path<String>,
) -> Result<Json<StatsResponse>, AppError> {
    let link = state
        .db
        .get_short_link(&short_code)
        .await?
        .ok_or(AppError::NotFound)?;

    let total_clicks = state.db.get_total_clicks(link.id).await?;
    let daily_clicks = state.db.get_daily_clicks(link.id, 7).await?;
    let top_referers = state.db.get_top_referers(link.id, 5).await?;

    Ok(Json(StatsResponse {
        total_clicks,
        daily_clicks,
        top_referers,
    }))
}

pub async fn redirect_short_link(
    State(state): State<Arc<AppState>>,
    Path(short_code): Path<String>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<Response, AppError> {
    let cached_link = state.cache.get_short_link(&short_code).await?;
    
    let link = if let Some(cached) = cached_link {
        if let Some(expires_at) = cached.expires_at {
            if Utc::now() > expires_at {
                return Err(AppError::Expired);
            }
        }
        
        state.cache.increment_click_count(cached.id).await?;
        
        Ok(Redirect::to(&cached.original_url).into_response())
    } else {
        let db_link = state
            .db
            .get_short_link(&short_code)
            .await?
            .ok_or(AppError::NotFound)?;
        
        if let Some(expires_at) = db_link.expires_at {
            if Utc::now() > expires_at {
                return Err(AppError::Expired);
            }
        }
        
        let cached = CachedShortLink {
            id: db_link.id,
            short_code: db_link.short_code.clone(),
            original_url: db_link.original_url.clone(),
            expires_at: db_link.expires_at,
        };
        state.cache.set_short_link(&cached, state.config.cache_ttl_seconds).await?;
        
        state.cache.increment_click_count(db_link.id).await?;
        
        Ok(Redirect::to(&db_link.original_url).into_response())
    };
    
    link
}
