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


---

## YouTube collector (`collectors/youtube/collect_gta.py`)

### Kya banaya aur kyun
Yeh ek daily batch script hai jo GTA 6 ke YouTube videos dhundhti hai, unke numbers ka roz ka snapshot leti hai, aur top videos ke comments save karti hai. GitHub Actions ([yt_daily.yml](../../.github/workflows/yt_daily.yml)) isse roz 00:30 UTC (06:00 IST) par chalata hai. Output teen tables mein jaata hai: `yt_videos`, `yt_video_stats`, `yt_comments`, aur har run ka record `collector_runs` mein.

### File walkthrough (important lines)

- **`QUERIES`, `SEARCH_COST`, `LIST_COST`, `QUOTA_BUDGET`** (top par): saare constants ek jagah. Budget 3000 hai jabki YouTube 10,000/day deta hai, matlab hum apni limit khud lagate hain.
- **`class Quota`**: `spend(n)` har API call se *pehle* chalta hai. Agar budget cross hone wala ho toh error deta hai, call hoti hi nahi. Isse bug se quota burn nahi hoga.
- **`api_get()`**: ek hi function se saari API calls. Dhyan do: URL mein `key=` hota hai, isliye hum URL kabhi log nahi karte. Error aaye toh sirf `reason` + `message` nikaal ke `YouTubeError` raise karte hain (`from None` se original traceback, jisme URL ho sakta hai, chhup jaata hai).
- **`db()`**: chhota context manager, connection sirf tab khulta hai jab DB kaam ho. API calls ke dauraan koi connection open nahi rehta (Neon compute hours bachte hain).
- **`search_videos()`**: har query ("GTA 6", "GTA VI") ke liye `search.list` with `publishedAfter = now - 24h`, `order=viewCount`, `maxResults=50`. `found.setdefault(...)` ka matlab: agar same video dono queries mein mila, pehli query `source_query` ban jaati hai, duplicate nahi.
- **`fetch_stats()`**: ids ko 50-50 ke batch mein `videos.list` ko bhejta hai. YouTube counts ko **string** mein deta hai (`"5517537"`), isliye `int()`. Agar koi field hidden hai (jaise likes) toh key hi gayab hoti hai, tab hum `None` (NULL) daalte hain. Jo video delete/private ho gaya woh response mein aata hi nahi, skip ho jaata hai.
- **`fetch_comments()`**: `commentThreads.list`, `order=relevance`, 50 comments. Agar comments band hain toh API 403 `commentsDisabled` deti hai. Hum us video ko skip karke aage badhte hain, poora run fail nahi hota.
- **`run()` ka flow**:
  1. `collector_runs` mein row `status='running'` (alag chhoti transaction, commit turant).
  2. Search, phir `yt_videos` upsert (`ON CONFLICT (video_id) DO UPDATE` sirf title/channel_title; `first_seen_at` aur `source_query` nahi badalte).
  3. `SELECT video_id FROM raw.yt_videos`: **saare** tracked videos, sirf aaj ke nahi.
  4. Stats, phir `yt_video_stats` insert, `ON CONFLICT (video_id, snapshot_date) DO UPDATE`. Same din dobara chalao toh us din ki row overwrite hoti hai, duplicate nahi.
  5. Top 10 videos by views ke comments, phir `yt_comments` upsert (`comment_id` primary key).
  6. `collector_runs` update: `success`, `rows_written`, aur `details` JSONB (rows per table, quota units).
  7. Koi bhi exception aaye toh `except` block run ko `failed` mark karta hai with error text, phir exit code 1 (taaki GitHub Actions bhi red dikhaye).
- **`--dry-run`**: API call karta hai (quota kharch hota hai) lekin DB ko bilkul nahi chhuta. `--limit N`: har search mein N results aur stats ke liye N videos tak, sasta testing ke liye.
- **[003_collector_runs_details.sql](../../sql/003_collector_runs_details.sql)**: `collector_runs` mein `details JSONB` column. `001` ko edit nahi karna tha (project rule), isliye nayi migration.

### Dry run: ek real video, API JSON se 3 tables tak
Video: `91c96JtcIFg`, "FIRST TO MAKE $1 MILLION IN GTA 6 WINS! ft. Kai Cenat" by IShowSpeed (25 Nov 2026 ko run).

1. **`search.list`** item ka relevant hissa (`snippet`): `videoId=91c96JtcIFg`, `channelId=UCWsDFcIhY2DBi3GB5uykGXA`, `channelTitle=IShowSpeed`, `publishedAt=2026-11-24T22:36:16Z`. Query "GTA 6" se mila.
   → **`raw.yt_videos`** row: `(video_id='91c96JtcIFg', title=..., channel_id='UCWsDFcIhY2DBi3GB5uykGXA', channel_title='IShowSpeed', published_at=2026-11-24 22:36:16+00, source_query='GTA 6', first_seen_at=2026-11-25 12:16:18)`. `first_seen_at` DB ka `now()` default hai.
2. **`videos.list`** `statistics`: `{"viewCount": "5517537", "likeCount": "63549", "commentCount": "633"}` (strings).
   → **`raw.yt_video_stats`** row: `('91c96JtcIFg', 2026-11-25, 5517537, 63549, 633, fetched_at=now())`. Kal ke run mein isi video ki nayi row `(..., 2026-11-26, ...)` judegi. Yahi time series hai.
3. Yeh video top-10 mein tha (views ke hisaab se), toh **`commentThreads.list`** chali. Ek item: `id=UgwxlXGB958tBMuhWw14AaABAg`, `textDisplay="Proof that you were here before the first 5 hours"`, `likeCount=820`, `publishedAt=2026-11-24T22:28:32Z`.
   → **`raw.yt_comments`** row: `(comment_id='UgwxlXGB958tBMuhWw14AaABAg', video_id='91c96JtcIFg', text=..., like_count=820, published_at=2026-11-24 22:28:32+00, fetched_at=now())`.

Is run ka log (`raw.collector_runs`, run_id=1): `status=success`, `rows_written=606`, `details={"rows": {"yt_videos": 73, "yt_video_stats": 73, "yt_comments": 460}, "quota_units": 212}`.

**Idempotency ka saboot:** usi din doosra run (run_id=2) chalaya. Table counts: videos 73 se 76, stats 73 se 76, comments 460 se 461. Yaani sirf 3 *naye* videos (24h window aage khisak gayi) aur 1 naya comment aaya. Purani rows dobara nahi bani.

### Quota math
- Search: 2 queries x 100 = **200**
- Stats: 73 videos / 50 = 2 batches x 1 = **2**
- Comments: top 10 videos x 1 = **10**
- Total = **212 units/day** (run_id=1 mein exactly yahi count hua).
- Roz ~70 naye videos judte hain, toh ek mahine baad ~2200 tracked, matlab 44 stats calls. Total ~254. Saal bhar baad ~27,000 videos, ~540 calls, total ~750. Limit 10,000 se bahut neeche. (Search ka kharcha fixed hai, sirf stats calls dheere-dheere badhte hain.)
- Agar ek din mein script 4-5 baar bhi chalao (har run 212), tab bhi ~1000. Safe.

### Search ek baar, stats roz kyun?
- **Search mehenga hai** (100 units vs 1) aur uska kaam hai *naye videos dhundhna*. Sirf pichhle 24h ke videos chahiye, isliye roz ek baar kaafi hai.
- **Stats sasta hai** (50 videos = 1 unit) aur numbers roz badalte hain. Purane video ke views trend dikhate hain (hype badh raha hai ya mar raha hai), isliye *saare* tracked videos ka roz snapshot.
- Agar roz search se "top videos" dobara dhundhte, toh purane viral videos list se gir jaate aur unka trend toot jaata. Alag karne se discovery (mehenga, sirf naya) aur tracking (sasta, sab kuch) ka kharcha alag control hota hai.

### 5 interview questions
1. **Idempotent kaise banaya?** Natural primary keys par `ON CONFLICT`: videos `video_id`, comments `comment_id`, stats `(video_id, snapshot_date)`. Same input dobara aaye toh update hota hai, duplicate nahi. Isiliye retry/re-run safe hai.
2. **`yt_video_stats` ko alag table kyun rakha?** Video ki metadata static hai, counts roz badalte hain. Append-only snapshot table se history bachti hai (growth = aaj minus kal), aur title jaisi strings roz repeat nahi hoti.
3. **Quota kaise manage kiya?** Costs ka hisaab (search 100, list 1), `Quota` class jo har call se pehle check karti hai aur 3000 ke budget par rok deti hai, batching (50 ids/call), aur comments sirf top 10 videos ke liye. Kul ~212 units/day.
4. **Run fail ho jaye toh kaise pata chalega?** `collector_runs` mein har run `running` se shuru hota hai aur `success`/`failed` + error text par khatam. Exception par script non-zero exit code deta hai, toh GitHub Actions bhi fail dikhata hai. Row `running` par atki mile toh script crash hui thi.
5. **API key secure kaise rakhi?** `.env` (gitignored) locally, aur GitHub Actions secrets CI mein. Code mein key kabhi hardcode nahi, aur URL (jisme key hoti hai) kabhi log nahi hota. Errors se sirf reason/message nikalte hain.
