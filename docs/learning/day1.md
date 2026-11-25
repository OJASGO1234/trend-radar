# Day 1 — Learning notes (Hinglish)

## 0. Database schema: kaun si table kya store karti hai

Saari tables Neon Postgres ke `raw` schema mein hain. "Raw" ka matlab: API se jo aaya, bilkul wahi store karo, koi cleaning/aggregation nahi. Cleaning baad mein dbt (Day 3) karega.

### `001_raw_schema.sql` — YouTube batch collector ki tables

| Table | Kya store karti hai | Primary key |
|---|---|---|
| `raw.yt_videos` | Har video ki ek row: title, channel, `published_at`, kis search query se mila (`source_query`), aur `first_seen_at` (humne pehli baar kab dekha) | `video_id` |
| `raw.yt_video_stats` | Video ke numbers (views, likes, comments) ka **daily snapshot** | `(video_id, snapshot_date)` |
| `raw.yt_comments` | Videos ke top comments: text, likes, kab post hua | `comment_id` |
| `raw.collector_runs` | Collector ke har run ka logbook | `run_id` (auto number) |

**`yt_video_stats` alag table kyun hai `yt_videos` se?**
Kyunki dono ka rate of change alag hai.
- Title, channel, published date **ek baar fix** ho jaate hain. Video ki ek row kaafi hai.
- View/like/comment counts **roz badalte hain**. Agar inhe `yt_videos` mein rakhte, toh roz update karne se purani value mit jaati aur hum kabhi nahi bata paate "GTA 6 trailer ke views kal se aaj tak kitne badhe".
- Alag table mein roz ek **nayi row** (append-only) aati hai. Primary key `(video_id, snapshot_date)` ka matlab ek video ki ek din mein sirf ek row. Collector same din dobara chale toh duplicate nahi banega (idempotent). Trend/growth nikalna ab simple hai: aaj minus kal.
- Storage bhi bachta hai: title jaisi lambi strings roz repeat nahi hoti, sirf 3 numbers + date.
- `video_id` ek foreign key hai `yt_videos` ki taraf, toh stats ki row tab hi bante jab video pehle se exist kare.

**`collector_runs` kis liye hai?**
Yeh pipeline ka "attendance register" hai. Collector shuru hote hi ek row daalta hai (`status='running'`, `started_at`), aur khatam hone par `finished_at`, `rows_written`, aur `status` ko `success` ya `failed` (+ `error` message) kar deta hai. `CHECK` constraint sirf in teen status ki ijazat deta hai. Isse hum jaan sakte hain: kya aaj ka GitHub Actions run chala? Kitni rows likhi? Fail hua toh kyun? Aur Day 3 mein Airflow/monitoring isi table se freshness check kar sakte hain. Ek run "running" par atka reh jaye toh matlab crash hua.

### `002_stream_schema.sql` — real-time (Kafka) ki table

| Table | Kya store karti hai |
|---|---|
| `raw.stream_events` | Bluesky, Wikipedia aur Hacker News ke saare keyword-matched events, ek hi table mein |

Important columns:
- `source` — `bluesky` / `wikipedia` / `hackernews` (CHECK se locked).
- `event_id` — us source ka apna unique id. **Primary key `(source, event_id)`** hai, isliye Kafka same message dobara de (at-least-once delivery) toh `ON CONFLICT DO NOTHING` duplicate rok deta hai.
- `event_time` — event asal mein kab hua; `ingested_at` — humne DB mein kab daala. Dono ka gap = pipeline ki delay.
- `text`, `url` — dikhane/search ke liye.
- `matched_keywords` — `TEXT[]` array: kaun se keywords match hue (jaise `{gta 6, rockstar}`).
- `topic_group` — `gta6` ya `ai_tech`. Yeh project ke do themes ko alag karta hai. (Pehle CLAUDE.md mein `topic_tag` likha tha; ab har jagah `topic_group` hai, schema source of truth hai.)
- `payload` — poora original event `JSONB` mein, taaki baad mein kuch aur nikalna ho toh dubara fetch na karna pade.
- `kafka_topic`, `kafka_partition`, `kafka_offset` — yeh event Kafka mein kahan tha. Debugging aur replay ke kaam aata hai.
- Index `(topic_group, event_time)` — "pichhle 1 ghante mein gta6 ke events" jaisi queries tez rakhta hai.

### Neon par abhi ki state (apply_schema.py ka output)

```
raw.collector_runs  rows=0
raw.stream_events   rows=0
raw.yt_comments     rows=0
raw.yt_video_stats  rows=0
raw.yt_videos       rows=0
```

Paanchon tables ban chuki hain, sab khali. Script dobara chalana safe hai kyunki har statement `IF NOT EXISTS` use karta hai.
