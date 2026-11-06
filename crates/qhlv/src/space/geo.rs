//! The geographic `Space`: objects live at (fuzzed) lat/lng, "near" means within
//! a radius in kilometres. This is the original hlv behaviour, now expressed as
//! one implementation of the pluggable `Space` trait.

use async_trait::async_trait;
use rand_distr::{Distribution, Normal};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::parse_env;
use crate::error::{Error, Result};
use crate::space::Space;

const EARTH_RADIUS_KM: f64 = 6371.0;
const GRID_METERS: f64 = 100.0;
const METERS_PER_DEGREE_LAT: f64 = 111_320.0;

/// Geographic proximity backed by a Redis GEO set.
pub struct GeoSpace {
    /// Default Gaussian jitter applied to posted coordinates (metres).
    pub default_sigma_m: f64,
    /// Upper bound on client-requested sigma (metres).
    pub max_sigma_m: f64,
    /// Maximum feed radius a client may request (kilometres).
    pub max_radius_km: f64,
    /// Redis key of the GEO set holding every live object's coordinates.
    geo_key: String,
}

/// What the client sends to place a post: real coordinates + optional noise.
#[derive(Deserialize)]
struct GeoLocator {
    lat: f64,
    lng: f64,
    noise_sigma: Option<f64>,
}

/// The stored point: fuzzed coordinates. Safe to return to clients as-is.
#[derive(Serialize, Deserialize)]
struct GeoPoint {
    lat: f64,
    lng: f64,
}

/// A client's viewport: where they are and how far they can see.
#[derive(Deserialize)]
struct GeoViewport {
    lat: f64,
    lng: f64,
    radius_km: f64,
}

impl GeoSpace {
    pub fn new(default_sigma_m: f64, max_sigma_m: f64, max_radius_km: f64) -> Self {
        Self {
            default_sigma_m,
            max_sigma_m,
            max_radius_km,
            geo_key: "feed:geo".to_string(),
        }
    }

    /// Load geo-specific settings from the environment, with protocol defaults.
    pub fn from_env() -> Self {
        Self::new(
            parse_env("DEFAULT_SIGMA_M", 300.0),
            parse_env("MAX_SIGMA_M", 1000.0),
            parse_env("MAX_RADIUS_KM", 10.0),
        )
    }
}

#[async_trait]
impl Space for GeoSpace {
    async fn locate(&self, locator: Value, _payload: &Value) -> Result<Value> {
        let loc: GeoLocator = serde_json::from_value(locator)
            .map_err(|e| Error::Validation(format!("invalid geo locator: {e}")))?;

        // Clamp requested noise to [0, max] so a client can't pick absurd values.
        let sigma = loc
            .noise_sigma
            .unwrap_or(self.default_sigma_m)
            .clamp(0.0, self.max_sigma_m);

        let (lat, lng) = fuzz_coordinates(loc.lat, loc.lng, sigma);
        Ok(serde_json::to_value(GeoPoint { lat, lng })?)
    }

    async fn index(&self, con: &mut ConnectionManager, id: &str, point: &Value) -> Result<()> {
        let p: GeoPoint = serde_json::from_value(point.clone())?;
        // GEOADD key longitude latitude member
        redis::cmd("GEOADD")
            .arg(&self.geo_key)
            .arg(p.lng)
            .arg(p.lat)
            .arg(id)
            .query_async::<_, ()>(con)
            .await?;
        Ok(())
    }

    async fn remove(&self, con: &mut ConnectionManager, id: &str) -> Result<()> {
        let _: i64 = con.zrem(&self.geo_key, id).await?;
        Ok(())
    }

    async fn query(&self, con: &mut ConnectionManager, viewport: &Value) -> Result<Vec<String>> {
        let vp: GeoViewport = serde_json::from_value(viewport.clone())
            .map_err(|e| Error::Validation(format!("invalid geo viewport: {e}")))?;
        let radius = vp.radius_km.clamp(0.0, self.max_radius_km);

        // GEOSEARCH returns member ids within the radius, nearest first.
        let ids: Vec<String> = redis::cmd("GEOSEARCH")
            .arg(&self.geo_key)
            .arg("FROMLONLAT")
            .arg(vp.lng)
            .arg(vp.lat)
            .arg("BYRADIUS")
            .arg(radius)
            .arg("km")
            .arg("ASC")
            .query_async(con)
            .await?;
        Ok(ids)
    }

    async fn all_indexed_ids(&self, con: &mut ConnectionManager) -> Result<Vec<String>> {
        // The GEO set never auto-expires; the sweep reconciles it against object keys.
        let ids: Vec<String> = con.zrange(&self.geo_key, 0isize, -1isize).await?;
        Ok(ids)
    }

    fn matches(&self, viewport: &Value, point: &Value) -> bool {
        // A malformed viewport/point simply doesn't match — never panic in fan-out.
        let (Ok(vp), Ok(p)) = (
            serde_json::from_value::<GeoViewport>(viewport.clone()),
            serde_json::from_value::<GeoPoint>(point.clone()),
        ) else {
            return false;
        };
        haversine_km(p.lat, p.lng, vp.lat, vp.lng) <= vp.radius_km
    }
}

/// Apply two-layer location fuzzing:
/// 1. Snap to a ~100m grid to remove sub-grid precision.
/// 2. Add Gaussian noise with the given sigma (metres).
pub fn fuzz_coordinates(lat: f64, lng: f64, noise_sigma_meters: f64) -> (f64, f64) {
    let mut rng = rand::thread_rng();

    // Layer 1: grid snap.
    let grid_deg_lat = GRID_METERS / METERS_PER_DEGREE_LAT;
    let grid_deg_lng = GRID_METERS / (METERS_PER_DEGREE_LAT * lat.to_radians().cos());
    let snapped_lat = (lat / grid_deg_lat).round() * grid_deg_lat;
    let snapped_lng = (lng / grid_deg_lng).round() * grid_deg_lng;

    // Layer 2: Gaussian jitter. sigma of 0 yields a zero-width distribution → no jitter.
    if noise_sigma_meters <= 0.0 {
        return (snapped_lat, snapped_lng);
    }
    let normal = Normal::new(0.0, noise_sigma_meters).expect("sigma > 0 checked above");
    let jitter_lat = normal.sample(&mut rng) / METERS_PER_DEGREE_LAT;
    let jitter_lng =
        normal.sample(&mut rng) / (METERS_PER_DEGREE_LAT * snapped_lat.to_radians().cos());

    (snapped_lat + jitter_lat, snapped_lng + jitter_lng)
}

/// Haversine distance in kilometres between two lat/lng points.
pub fn haversine_km(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_KM * a.sqrt().asin()
}
