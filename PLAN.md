# qhlv — generalization plan

**qhlv = "que hablen los vecinos"** — subjunctive: *"may the neighbors speak" / "let the
neighbors speak."* An invitation, not the indicative "the neighbors talk." (The hlv draft
`API.md` has the wrong form, "hablan" — to be corrected.)

Extract the core of [`hlv`](https://github.com/alpad5/hlv) into reusable pieces so more
apps can be built on the same principle: **anonymous, ephemeral, proximity-scoped,
real-time objects**.

This working directory (`/Users/alex/Documents/qhlv`) becomes the **protocol repo /
workspace root** — it holds `qhlv`. The hlv clone under `./hlv` is reference only.

Decisions (locked):
- **Artifact:** shared code core + client SDK (not a language-agnostic spec effort).
- **Data model:** opaque app-defined payload; keep the *object + life-extending activity*
  shape (hlv's thread + comment, renamed and made payload-generic).
- **Tenancy:** one qhlv server per app, each with its own Redis.
- **Proximity is pluggable** via a `Space` trait. Carve the seam now, implement geo first.
- **Hybrid query**: a viewport = anchor + threshold + metadata facet filters.
- **Phase 3 stress-test app is the semantic one.**
- **Wire events are generic:** `object_created` / `activity_added` / `object_expired`.
- **Core API:** ship `qhlv::router(...)` *and* expose the underlying store/ws/space functions.
- **Endpoints (generic):** `POST /objects`, `POST /feed` (viewport as JSON body, so arbitrary
  space shapes work), `POST /objects/:id/activities`, `GET /objects/:id/activities`, `GET /ws`.
- **Point/Viewport are opaque JSON to the core** (`serde_json::Value`), interpreted by the
  active `Space` — same philosophy as the opaque payload. Core uses `Arc<dyn Space>`.

---

## The principle (what every qhlv app shares)

1. **No identity** — anonymous by construction.
2. **No history** — every object is TTL-bound: an inactivity timer + a hard cap.
3. **Proximity-scoped** — an object occupies a point in *some space*; you see the objects
   near *your* point, under that space's metric and threshold. **Geo is one instance.**
4. **Liveness through interaction** — an activity resets the parent's inactivity timer.
5. **Push, not poll** — real-time fan-out to every client whose viewport contains the event.

> Note: *privacy-preserving location fuzzing* is **not** a universal invariant — it's the
> geo space's `locate` transform. Other spaces may have no equivalent (in the semantic
> space, the text *is* the position and is already public to its neighbors).

## The core insight

The engine's real job — TTL + hard cap, activity-resets-the-timer, WS fan-out mechanics,
anonymity, opaque payload, the expiry sweep — is **orthogonal to what space objects live
in.** Swap the entire notion of "where things are" and that machinery is untouched. So
generalization has two moves:

1. `content: String` → opaque `payload: Value`; `Thread`/`Comment` → `Object`/`Activity`.
2. Geo indexing/query/fan-out → a pluggable **`Space`**.

---

## The `Space` abstraction

A pluggable proximity provider. Everything geo-specific in today's backend
(`GEOADD`/`GEOSEARCH`, haversine fan-out, coordinate fuzzing) lives behind it.

```rust
/// A pluggable proximity space. GeoSpace is one impl; SemanticSpace is another.
#[async_trait]
pub trait Space {
    /// Derive the object's stored point from the client's raw locator + payload.
    /// Geo: fuzz lat/lng. Semantic: embed the text (+ derive facet metadata).
    async fn locate(&self, input: LocatorInput, payload: &Value) -> Result<Point>;

    async fn index(&self, id: &str, point: &Point) -> Result<()>;
    async fn remove(&self, id: &str) -> Result<()>;

    /// The feed: nearest-neighbors within threshold, AND matching facet filters.
    async fn query(&self, viewport: &Viewport) -> Result<Vec<String>>;

    /// The WS fan-out membership test for a single connected client.
    fn matches(&self, viewport: &Viewport, point: &Point) -> bool;
}
```

- **`Point`** — opaque, stored alongside the object (geo: `(lat,lng)`; semantic: an
  embedding vector + facet metadata). Serialized into the Redis object record.
- **`Viewport`** — `{ anchor: Point, threshold, filters: Vec<FacetFilter> }`. This is the
  **hybrid query**: an ANN/metric part *plus* metadata constraints.
  - Geo: anchor = coords, threshold = radius_km, filters usually empty.
  - Semantic: anchor = topic vector, threshold = cosine similarity, filters =
    `sentiment ∈ [x,y]`, `positivity ≥ p`, etc. — evaluated as metadata filters, not fused
    into the vector.
- **`LocatorInput`** — the raw client signal. Symmetry across spaces: *client sends a raw
  signal, server derives the point.* Geo: lat/lng. Semantic: text + affect params.

| | **GeoSpace** | **SemanticSpace** |
|---|---|---|
| `Point` | `(lat, lng)` | embedding vector + facets |
| `locate` | grid-snap + Gaussian fuzz | embed text; derive sentiment facets |
| index/query | `GEOADD` / `GEOSEARCH BYRADIUS` | vector KNN + metadata filter (hybrid) |
| `matches` | `haversine ≤ radius` | `cosine ≥ threshold` (+ filter check) |

**Redis stays the store** either way (object hash + TTL + activities list + sweep). For the
semantic space, Redis Stack's vector search can serve as the index too — so a semantic app
may need **no new datastore**, only a different index type. An external vector DB remains an
option the `Space` impl can choose.

---

## Target architecture

```
qhlv/                          # THIS repo — cargo + npm workspace root
  crates/
    qhlv/                 # the engine + Space trait + GeoSpace
  apps/
    hlv/                       # first consumer: messages + replies (geo)
    <semantic-app>/            # Phase 3: SemanticSpace + hybrid sentiment query
  packages/
    qhlv-client/               # JS/TS SDK (HTTP + WS)
  hlv/                         # reference clone (git-ignored / not part of workspace)
```

### `qhlv`

Payload-generic lifecycle types (engine owns lifecycle, app owns payload):

```rust
pub struct Object {
    pub id: String,
    pub point: Point,             // opaque; interpreted by the active Space
    pub created_at: i64,
    pub expires_at: i64,          // hard cap
    pub last_activity: i64,
    pub activity_count: u64,
    pub payload: serde_json::Value,
}
pub struct Activity { pub id: String, pub object_id: String, pub created_at: i64, pub payload: serde_json::Value }
```

Public surface:
- `Config` (TTLs, radius/threshold + content caps) — from env.
- Store ops: `save_object`, `add_activity` (Lua TTL-reset, unchanged), `get_object`,
  `get_activities`, `get_feed`, `sweep_expired` — all delegate spatial work to the `Space`.
- `Space` trait + `GeoSpace` impl (fuzzing lives in `GeoSpace::locate`).
- WS fan-out + generic events: `ObjectCreated { data }`, `ActivityAdded { object_id, data }`,
  `ObjectExpired { object_id }`.
- **`qhlv::router(config, space, validate) -> axum::Router`** — ready-to-mount router
  with the five endpoints + `/ws`. App hooks: the `Space` impl and a payload `validate` fn.

App-specific surface shrinks to: **config values + a `Space` choice + a payload validator +
a frontend.**

### `qhlv-client` (JS/TS SDK)

Generalize `hlv/frontend/src/lib/api.js`:
```js
const c = createClient({ apiBase });
await c.postObject(payload, locator, { /* space-specific opts */ });
await c.getFeed(viewport);          // geo: {lat,lng,radiusKm}; semantic: {topic, threshold, filters}
await c.postActivity(objectId, payload);
await c.getActivities(objectId);
c.connect(viewport, onEvent);       // WS + viewport updates
```
Framework-agnostic; optional Svelte-store wrapper later.

---

## Phased plan

**Phase 1 — Stand up the qhlv workspace + `qhlv` (geo only).**
- Init the workspace in this repo; create `crates/qhlv`.
- Move `geo`/`config`/`store`/`ws` in; introduce the `Space` trait and `GeoSpace`
  (fuzzing moves into `GeoSpace::locate`). Make store + models payload-generic.
- Port hlv into `apps/hlv` as a thin consumer: `Config` + `GeoSpace` + a `validate`
  (payload `content` length) + mount `router`.
- **Parity check:** hlv's external behavior matches today's (WS variant rename is a
  deliberate wire change — handled with the frontend in Phase 2).

**Phase 2 — `qhlv-client` SDK + wire up hlv's frontend.**
- Extract api.js into the SDK; hlv frontend consumes it. Confirms the SDK fits a real app.

**Phase 3 — Semantic app (the real generalization test).**
- New app implementing `SemanticSpace`: `locate` embeds message text and derives sentiment
  facets; `query` does hybrid KNN + `sentiment`/`positivity` filters; index via Redis Stack
  vector search (or an external vector DB).
- If `qhlv` needs **zero** changes to host it, the seam was cut in the right place.

**Phase 4 — Ergonomics.**
- `create-qhlv-app` scaffold; document the `Space` + payload/validator contract in
  `qhlv`'s README.

## Open design decisions (before/within Phase 1)

- **Async `locate`:** embedding makes `locate` async + adds an external dependency and
  latency on the post path. Geo's `locate` is local/free. Trait is async either way; note
  the cost for semantic.
- **Facet declaration (semantic):** how an app declares which facets exist and how they're
  derived from a payload (sentiment model? client-supplied?). Lives in the `Space` impl.
- **Embedding provider/model** for the semantic app (Phase 3 decision).

## What we are NOT doing (for now)

- No formal language-agnostic spec / conformance kit.
- No multi-tenant server.
- No fully-generic object graph — strictly object + activity.
- No changes to the original hlv repo; this workspace is the target.
