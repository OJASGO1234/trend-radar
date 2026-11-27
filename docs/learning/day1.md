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



---

## Kafka setup (`infra/docker-compose.kafka.yml`)

### Kya banaya aur kyun
Ek single Kafka broker Docker mein, KRaft mode mein. 3 bursty producers (Bluesky, Wikipedia, HN) aur DB writer ke beech Kafka buffer ka kaam karta hai: producer tez ho ya DB slow ho, events kho nahi jaate, aur baad mein naye consumers (spike detector, LLM enrich) bina producer chhede attach ho sakte hain. Compose file mein do services hain: `kafka` (broker, `mem_limit: 900m`, heap `-Xmx512m -Xms512m`) aur `kafka-init` (one-shot job jo 3 topics banake exit ho jaata hai, `--if-not-exists` ki wajah se dobara chalana safe hai).

Topics: `raw.bluesky`, `raw.wikipedia`, `raw.hackernews`, har ek 3 partitions, replication 1, `retention.ms=259200000` (3 din). `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` hai, taaki producer mein spelling mistake se chupchaap naya topic na ban jaye.

### Test result (real)
- Broker `healthy`, `Test-NetConnection localhost -Port 9092` = True (Windows host se reachable).
- Message `hello from trend-radar` ko `raw.hackernews` mein produce kiya, phir consume: `Partition:0  Offset:0  hello from trend-radar`.
- `docker stats`: `kafka  389.7MiB / 900MiB  (43%)`. Matlab heap 512m + JVM ka non-heap (metaspace, buffers) milake ~390 MiB idle; 900m limit ke andar comfortable.
- (Message ke aage ek invisible BOM character dikha: yeh PowerShell 5.1 ka pipe encoding quirk hai, Kafka ka nahi. Python producers mein yeh issue nahi aayega.)

### Compose file ke important lines
- `KAFKA_PROCESS_ROLES: broker,controller`: ek hi process dono kaam karta hai (combined mode). Chhote setup ke liye perfect.
- `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093`: controller quorum mein sirf ek voter, node id 1, port 9093.
- `CLUSTER_ID` fixed: data volume (`kafka-data`) ke andar cluster id likha hota hai. Agar har baar naya ID aaye toh purane volume ke saath broker start hi nahi hoga.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` (aur transaction log wale do): Kafka ke internal topics ka default replication 3 hota hai; ek broker par 3 replicas nahi ban sakte, toh consumer groups kabhi kaam hi nahi karte. Yeh classic single-node bug hai.
- `KAFKA_LISTENERS` / `KAFKA_ADVERTISED_LISTENERS`: neeche detail mein.
- `healthcheck` + `depends_on: condition: service_healthy`: init job tab chalta hai jab broker sach mein ready ho.

### KRaft kya replace karta hai: ZooKeeper
Pehle Kafka ko cluster ka metadata (kaun broker zinda hai, kis partition ka leader kaun, topic configs) store karne aur leader election ke liye alag ZooKeeper cluster chahiye tha. Matlab 2 systems chalao, monitor karo, secure karo. **KRaft** (Kafka Raft) mein Kafka ke andar hi controller nodes hain jo Raft consensus se yeh metadata ek internal log (`__cluster_metadata`) mein rakhte hain. Fayde: ek system kam, tez startup, aur hamare 3 GB Docker limit mein ek aur JVM nahi chalani padti. Kafka 4.0 se ZooKeeper support hata diya gaya hai.

### Partitions, offsets, consumer groups
- **Topic** = ek naam wala event log (jaise `raw.bluesky`).
- **Partition** = topic ka ek ordered, append-only hissa. Topic ko 3 partitions mein baanta hai taaki parallelism mile: 3 consumers ek saath alag partitions padh sakte hain. Order sirf **ek partition ke andar** guarantee hota hai, poore topic mein nahi. Message ki key ka hash decide karta hai kaun si partition (same key = same partition).
- **Offset** = partition ke andar message ka sequence number (0, 1, 2...). Hamare test mein `Partition:0 Offset:0`. Consumer apna "kahan tak padha" offset yaad rakhta hai, aur isiliye replay possible hai (offset peeche karo, dobara padho).
- **Consumer group** = consumers ka ek naamed group. Kafka topic ki partitions group ke members mein baant deta hai (har partition ek time par group ke sirf ek member ko). Group ke committed offsets `__consumer_offsets` topic mein save hote hain. Alag group = alag independent reader, jo poora data dobara padh sakta hai. Hamare project mein `db_sink` ek group hoga; baad mein spike detector doosra group.
- Hamari delivery rule: DB insert **ke baad** offset commit. Crash hua toh message dobara aayega (at-least-once), aur DB ka `ON CONFLICT DO NOTHING` duplicate rok dega.

### replication=1 yahan theek kyun, prod mein kyun nahi
Replication factor = ek partition ki kitni copies alag brokers par. Hamare paas **ek hi broker** hai, toh 2+ copies ban hi nahi sakti, aur laptop par dev data hai (raw events dobara collect/replay ho sakte hain, aur Neon mein bhi copy hai). Prod mein replication 3 + `min.insync.replicas=2` hota hai: ek broker mar jaye toh bhi data aur availability bachi rehti hai. Hamare yahan broker mara toh us waqt Kafka band, aur disk kharab hui toh 3 din ke un-sunk events gaye. Hum is risk ko jaan ke accept karte hain.

### advertised.listeners itna kyun toot-ta hai
Client pehle broker se connect hota hai (bootstrap address), broker use metadata mein **advertised address** deta hai, aur client aage ke saare connections usi address par karta hai. Agar advertised address client ke liye reachable nahi hai, toh pehla connect kaam karega par produce/consume hang ya timeout ho jaayega: sabse confusing bug.
- Windows host se: broker `localhost:9092` advertise karta hai. Host par yeh sahi hai.
- Lekin Docker ke andar doosre container ke liye `localhost` = *woh container khud*, broker nahi. Isliye ek **alag INTERNAL listener** `kafka:29092` (Docker DNS naam) advertise karte hain.
- Toh ek hi broker ke do naam: host ke liye `localhost:9092`, containers ke liye `kafka:29092`. Client jis listener par connect hota hai, usi ka advertised naam wapas milta hai.
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` mein har listener naam ka protocol map karna padta hai, aur `KAFKA_LISTENERS` (kahan sunna hai, `0.0.0.0`) alag cheez hai `ADVERTISED_LISTENERS` (kya batana hai) se. Yeh do settings mix hona hi sabse common galti hai.
- Hamare setup mein isi wajah se init job `kafka:29092` use karta hai aur Windows scripts `localhost:9092`.

### 5 interview questions
1. **KRaft vs ZooKeeper?** KRaft mein metadata Kafka ke apne controller quorum mein Raft log se manage hota hai, alag ZooKeeper cluster nahi. Kam ops, tez failover/startup, aur zyada partitions scale hoti hain. ZooKeeper Kafka 4.0 mein remove ho gaya.
2. **Ordering kahan guarantee hai?** Sirf ek partition ke andar. Same entity ka order chahiye toh usi key se produce karo, taaki hash se same partition mile.
3. **Consumer group kaise kaam karta hai?** Group ke members partitions baant lete hain (ek partition = group mein ek consumer). Zyada consumers than partitions = kuch idle. Group ke committed offsets Kafka mein store hote hain, aur alag group independent reader hai.
4. **At-least-once kaise mili, duplicates kaise sambhale?** Offset DB insert ke baad commit; crash par message replay. DB mein `(source, event_id)` primary key + `ON CONFLICT DO NOTHING`, toh effect exactly-once jaisa hai.
5. **Docker mein Kafka connect kyun fail hota hai?** `advertised.listeners` galat: client bootstrap par connect hota hai, par broker jo address wapas deta hai woh client ke network se reachable nahi (jaise container ke andar `localhost`). Solution: alag listeners, host ke liye `localhost:9092`, containers ke liye `kafka:29092`.

---

## Streaming producers (`streaming/common/` + `streaming/producers/`)

### Kya banaya aur kyun
Teen producers banaye jo live data Kafka mein daalte hain: **Bluesky** (WebSocket), **Wikipedia** (SSE), **Hacker News** (polling). Teeno ek hi `streaming/common/` code share karte hain: config, keyword filter, event envelope, Kafka helper (`acks=all`, idempotence on, key = `event_id`) aur ek runtime jo `--dry-run`, `--limit`, `--duration` flags aur har 30 sec ka `seen / kept / sent` log deta hai. Har producer **Kafka se pehle** keywords se filter karta hai, kyunki Neon free tier mein storage kam hai (neeche explain).

### Real test result (har producer ~2 min)
| Producer | seen | kept | sent | failed |
|---|---|---|---|---|
| bluesky | 5,684 | 12 | 12 | 0 |
| wikipedia (live) | 4,282 | 0 | 0 | 0 |
| hackernews (live, poll 20s) | 13 | 0 | 0 | 0 |

Wikipedia aur HN ka live data 2 min mein kuch match nahi hua: enwiki titles mein keyword wale pages rare hain, aur HN par ~6 items/min aate hain. Isliye maine **saved state peeche kar di** (jaise producer 2 ghante / ~450 items downtime ke baad restart hua ho): Wikipedia ne `Last-Event-ID` se ~2 ghante replay kiya (175,347 events seen, 12 kept), HN ne 469 items fetch kiye (31 kept). Yeh crash-restart path ka real test bhi tha.

### File-by-file walkthrough
- `common/config.py`: `.env` se `KAFKA_BOOTSTRAP`, `HN_POLL_SECONDS`, `WIKI_USER_AGENT`, `STATE_DIR` padhta hai. Secret kuch nahi hai yahan.
- `common/keywords.py`: `config/keywords.yaml` se ek regex per topic_group banata hai. `(?<!\w)...(?!\w)` ka matlab **whole word**: `rag` "storage" ya "fragile" mein match nahi hoga. Lambe phrase pehle (`gta 6 trailer` > `gta 6`). Dono groups match hon toh jis group ke zyada distinct keywords, tie par `gta6`.
- `common/envelope.py`: `make_event()` CLAUDE.md wala envelope banata hai (`event_id, source, topic_group, event_ts, ingested_ts, payload`). `payload` mein hamesha `text`, `url`, `matched_keywords` aur ek chhota `raw` copy: sink isi se `raw.stream_events` ke columns bharega.
- `common/kafka_helper.py`: `acks=all` (saare in-sync replicas confirm karein), `enable.idempotence=True` (retry se broker ke andar duplicate nahi), key = `event_id` (same event same partition). `on_delivery` callback `sent` / `failed` count karta hai: **sent tabhi gina jata hai jab broker ne confirm kiya**, `produce()` call par nahi.
- `common/state.py`: cursor file **atomically** likhta hai (tmp file + `os.replace`), taaki crash beech mein half file na chhode.
- `common/backoff.py`: reconnect delay 1s, 2s, 4s... max 60s, random jitter ke saath (sab clients ek saath na tootein).
- `common/runtime.py`: Ctrl+C par `stop` flag, 30s stats thread, exit par `flush()`.
- `producers/bluesky.py`: Jetstream URL mein `wantedCollections=app.bsky.feed.post` (server hi sirf posts bhejta hai). Sirf `operation == create` aur `langs` mein `en`. `event_id = did/rkey` (author + record key = post ki unique pehchaan). `time_us` har message par cursor banta hai.
- `producers/wikipedia.py`: `User-Agent` header zaroori (Wikimedia policy). `wiki == "enwiki"`, type `edit`/`new`, **sirf title** par keyword match. `event_id = meta.id` (UUID). Har event ka SSE `id:` yaad rakhta hai aur reconnect par `Last-Event-ID` header mein bhejta hai.
- `producers/hackernews.py`: `maxitem` poll karta hai, `last_id+1 .. maxitem` ke items fetch (8 threads), story/comment filter, deleted/dead skip. Pehli baar chalne par "ab se" shuru (backfill nahi), aur lambe downtime ke baad max 500 items catch-up.

### Filter Kafka se PEHLE kyun (Neon storage)
Bluesky par ~47 posts/sec aati hain (5,684 in 2 min), jinme se sirf ~0.2% hamare keywords wali hain (12). Agar sab Kafka se Neon tak jaata toh ~4 million rows/din: Neon free tier ka storage kuch ghanton mein bhar jaata. Filter producer mein lagane ke teen fayde: (1) **Kafka disk aur network** bachta hai, (2) **Neon storage + compute hours** bachte hain (consumer kam batch insert karta hai, connection kam der khula), (3) laptop ka CPU/RAM bachta hai. Nuksaan: agar baad mein naya keyword add karein toh purana data nahi milega, kyunki jo drop hua woh gaya. Isliye keywords soch kar rakhe hain.

### Push (WebSocket/SSE) vs Pull (polling)
- **Push**: connection ek baar khulta hai, server data aate hi bhejta hai. Latency kam, request overhead nahi. Bluesky (WebSocket, dono taraf baat ho sakti hai) aur Wikipedia (SSE, sirf server → client, plain HTTP par, `Last-Event-ID` se resume built-in). Nuksaan: connection tootega, reconnect + resume khud sambhalna padta hai.
- **Pull**: hum khud poochte hain "kuch naya?". HN ka Firebase API push nahi deta, toh `maxitem` poll karte hain. Simple aur crash-proof (state = ek number), par latency = poll interval (`HN_POLL_SECONDS`), aur har poll par request cost. Trade-off: poll interval kam = fresh data par zyada requests.

### Crash / restart par kya hota hai
Common rule: **state file tabhi aage badhti hai jab `flush()` confirm kar de ki broker ne messages le liye.** Isliye state hamesha "Kafka mein pahunche hue" se peeche ya barabar rehti hai, kabhi aage nahi. Result: at-least-once, kuch events dobara aa sakte hain, par DB ka `ON CONFLICT DO NOTHING` unhe rok dega.
- **Bluesky**: `.state/bluesky_cursor.json` mein `time_us` (har 10s par flush ke baad save). Restart par Jetstream URL mein `cursor=` lagate hain, **2 sec peeche** se, toh beech ke events replay hote hain. Jetstream sirf kuch ghante ka replay rakhta hai; usse lamba downtime = gap. Connection toote par: backoff ke saath auto-reconnect, usi cursor se.
- **Wikipedia**: `.state/wikipedia_last_event_id.json` mein SSE `id`. Restart par `Last-Event-ID` header bhejte hain, Wikimedia us point se replay karta hai (kuch din ki history). Hamare test mein 2 ghante peeche se 175k events replay hue.
- **Hacker News**: `.state/hackernews_last_id.json` mein last item id. Restart par `last_id+1..maxitem` fetch hote hain, toh poll ke beech ka kuch miss nahi hota (max 500 items). Agar koi item fetch fail ho toh state **us se aage nahi badhti**, next poll par wohi dobara try hota hai.
- Kisi bhi producer ka state file delete kar do toh: Bluesky aur Wikipedia "ab se" shuru, HN "maxitem se" shuru. Kuch crash nahi hota, bas beech ka data nahi aata.

### Ek real record ka safar (dry run): Wikipedia
1. Wikimedia SSE se event aata hai: `wiki=enwiki`, `type=edit`, `title="Grand Theft Auto VI"`, user `JT0219`, `meta.id=f6bd1adb-9348-4871-827e-f93c7f1bf283`.
2. `to_event()`: enwiki + edit pass; `kf.match("Grand Theft Auto VI")` → `("gta6", ["grand theft auto vi"])`.
3. Envelope: `event_id=f6bd1adb-...`, `topic_group=gta6`, `event_ts=2026-11-26T15:02:10.000+00:00`, `payload.text="Grand Theft Auto VI"`, `payload.url=https://en.wikipedia.org/w/index.php?diff=1388089449&oldid=1388088829`.
4. Kafka: topic `raw.wikipedia`, key = `event_id`, **Partition 0, Offset 0**; `acks=all` confirm ke baad `sent` +1.
5. Aage (db_sink, abhi nahi bana): yeh `raw.stream_events` mein `(source=wikipedia, event_id=f6bd1adb-...)` ban kar jayega; dobara aaye toh `ON CONFLICT DO NOTHING`.

### 5 interview questions
1. **Filter Kafka se pehle kyun, DB mein kyun nahi?** Volume 99.8% noise hai; pehle filter se Kafka disk, network aur Neon storage/compute bachta hai. Trade-off: jo drop hua woh replay nahi ho sakta, toh keywords soch kar rakhne padte hain.
2. **`acks=all` + idempotence kya dete hain?** `acks=all`: write tabhi success jab saare in-sync replicas ke paas ho (data loss se bachav). Idempotence: producer retry par broker sequence numbers se duplicate drop karta hai (ek session ke andar). Hamare 1 broker par `acks=all` ka fayda limited hai, par prod config ki aadat banti hai. Restarts ke beech ke duplicates DB ke unique `event_id` se handle hote hain.
3. **WebSocket vs SSE vs polling kab?** WebSocket: dono taraf real-time baat. SSE: sirf server→client, HTTP par, auto-reconnect aur `Last-Event-ID` built-in. Polling: jab source push nahi deta (HN) ya simple/stateless chahiye; latency = interval.
4. **Producer crash kare toh data loss kaise rokte ho?** State (cursor/last id) sirf `flush()` ke baad save hoti hai, toh state hamesha Kafka mein confirmed data se peeche rehti hai. Restart par source se wahin se replay (Jetstream cursor, SSE `Last-Event-ID`, HN id range). Result at-least-once; dedupe DB mein `event_id` unique + `ON CONFLICT DO NOTHING`.
5. **Message key `event_id` kyun?** Same key = same partition, aur load hash se barabar baantta hai. Dhyan: ordering sirf ek key ke andar milti hai. Hamein global order nahi chahiye, kyunki analysis `event_ts` se hota hai.

---

## Streaming consumer (`streaming/consumers/db_sink.py`)

### Kya banaya aur kyun
`db_sink` ek Kafka consumer hai (group `db_sink`) jo teeno topics (`raw.bluesky`, `raw.wikipedia`, `raw.hackernews`) padhta hai aur Neon ki `raw.stream_events` table mein **batch insert** karta hai (500 rows ya 5 sec, jo pehle ho). Producers aur DB ke beech Kafka isliye hai ki producers tez/bursty hain, aur DB writer apni raftaar se chale; DB down ho toh events Kafka mein ruke rehte hain, kuch khota nahi. Sink ka rule: **pehle DB commit, uske baad hi Kafka offset commit.** Isse at-least-once milta hai, aur `ON CONFLICT DO NOTHING` duplicates ko harmless bana deta hai. Ek aur zaroori cheez: kharab (poison) message se sink kabhi crash nahi hota.

> Dhyan: CLAUDE.md mein `ON CONFLICT (event_id)` likha hai, par table ka primary key `(source, event_id)` hai (`sql/002_stream_schema.sql`). ON CONFLICT ka target PK se match karna padta hai, isliye code mein `ON CONFLICT (source, event_id)` hai. Do alag sources ka same event_id ab bhi alag rows hain.

### File-by-file walkthrough (important lines)
- `Consumer({... "enable.auto.commit": False, "auto.offset.reset": "earliest"})`: auto-commit band, kyunki auto-commit time par offset aage badha deta hai, chahe DB insert hua ho ya nahi (loss ka rasta). `earliest` ka matlab: jab group ka koi committed offset na ho (pehli run), toh topic ke shuru se padho.
- `parse_message()`: ek message ko `Row` mein badalta hai ya `Poison` raise karta hai. Checks: valid UTF-8 JSON, JSON object, `source` known ho aur **usi topic ka ho** (`raw.bluesky` par `source=wikipedia` nahi), `event_id` non-empty string, `topic_group in (gta6, ai_tech)` (DB ke CHECK jaisa), `event_ts` parse ho, `payload` object ho, `text/url/matched_keywords` sahi type ke. Yaani DB ke CHECK/NOT NULL se pehle Python hi pakad leta hai.
- `Sink._handle()`: har message par `offsets[(topic, partition)] = max(offset)` update hota hai, **poison par bhi**. Poison ko "done" maante hain (log karke), warna wohi message baar baar atakta rehta.
- `_batch_due()`: `len(rows) >= 500` ya batch ke pehle message ko 5 sec ho gaye. Idle ho toh koi DB connection nahi khulta (Neon compute hours bachte hain).
- `insert_batch()`: ek hi multi-row `INSERT ... VALUES (...), (...), ... ON CONFLICT (source, event_id) DO NOTHING` ek transaction mein. `cursor.rowcount` = sach mein jitni rows insert hui; `duplicates = batch - inserted`.
- `insert_isolating()`: agar poore batch ko database `DataError` de (jaise text mein NUL `\x00`, Postgres allow nahi karta), toh row-by-row try karte hain taaki ek kharab row baaki 499 ko na rok de. Kharab row poison count hoti hai.
- `_write_with_retry()`: Neon down / network error (`OperationalError`) par **offsets commit nahi karta**, backoff (1s tak 30s cap, jitter) ke saath dobara try karta hai. `max.poll.interval.ms=600000` rakha hai taaki lamba outage hone par Kafka group se na nikaale.
- `_commit_offsets()`: Kafka mein committed offset = "agla padhne wala message", isliye `offset + 1`. Synchronous commit.
- Shutdown: Ctrl+C / SIGTERM / `.state/db_sink.stop` file, loop ruk jaata hai, bacha batch flush, offsets commit, `consumer.close()` (group se saaf exit).
- `--dry-run`: parse + validate karta hai, DB/commit nahi, aur alag group `db_sink-dryrun` use karta hai taaki asli sink ke offsets ya partitions na chhedein. `--limit N` / `--duration S` testing ke liye.

### Real test result (laptop par, Neon free tier)
**1) Poison:** topic `raw.hackernews` partition 0 offset 0 par ek purana "hello from trend-radar" message tha (UTF-8 BOM ke saath). Sink ne usse purge nahi kiya, bas log kiya aur aage badh gaya:
`WARNING db_sink poison topic=raw.hackernews partition=0 offset=0 reason=not JSON (JSONDecodeError) head=b'\xef\xbb\xbfhello from trend-radar'`

**2) 5+ minute end-to-end (3 producers + sink):** Neon mein final counts:

| source | topic_group | rows |
|---|---|---|
| bluesky | ai_tech | 3,024 |
| bluesky | gta6 | 3,299 |
| hackernews | ai_tech | 83 |
| wikipedia | ai_tech | 32 |
| wikipedia | gta6 | 25 |
| **total** | | **6,463** |

(Bluesky itna zyada isliye hai ki saved cursor kal shaam ka tha, toh Jetstream ne ~16 ghante ka backlog replay kiya. Iska matlab Neon storage jaldi bharti hai; cursor purana ho toh `.state/bluesky_cursor.json` delete karke "ab se" shuru karo.)

**3) Bina plan ka fault test:** run ke beech laptop/Docker network ~2 min atak gaya (Kafka fetch 101s timeout, Neon connection abort 10053). Consumer group se nikal gaya (`session timed out`), offset commit `UNKNOWN_MEMBER_ID` se fail hue, aur rejoin ke baad wahi batch dobara padha gaya: **60 duplicates, DB ne sab ignore kiye, 0 loss.** Yeh at-least-once ka asli saboot tha.

**4) Sink ko kill kiya (`taskkill /F`, koi flush/commit nahi), ~90 sec baad restart:** committed offsets se aage padhna shuru hua (lag 136+147+130 Bluesky partitions par), 1,666 naye rows aaye, 0 duplicates. (Kill do batches ke beech laga tha, isliye replay nahi hua; replay ka saboot test 3 aur 5 hain.)

**5) Forced replay:** `kafka-consumer-groups --reset-offsets --to-earliest` se group ko offset 0 par lautaya aur sink dobara chalaya. Saare 6,466 messages dobara padhe gaye: `inserted=0 duplicates=6465 poison=1`. DB phir bhi 6,463 rows.

**6) Verification (`python -m scripts.verify_sink`)**: Kafka ko 0 se end tak padhta hai aur Neon se milata hai:

```
kafka: distinct valid (source,event_id)  : 6463
neon : rows in raw.stream_events         : 6463
MISSING in DB (loss)                     : 0
EXTRA in DB (not in Kafka)               : 0
rows pointing at a non-existent offset   : 0
consumer lag: 0 on all 9 partitions
RESULT: OK - no loss, no duplicates
```
Kafka mein `raw.bluesky` ke 6,325 messages the par distinct valid events 6,323: 2 duplicates Kafka mein hi the (producer reconnect par cursor 2s peeche karta hai). DB mein woh ek hi baar hain.

### Ek real Bluesky post ka poora safar (dry run)
**Step 1: Jetstream WebSocket frame.** `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&cursor=...` par yeh JSON aata hai (kuch fields chhote kar diye; `time_us` event_time se nikaala hai, baaki values real row ki hain):
```json
{"did": "did:plc:eekr2bqpfbdms77jwngpvfvs",
 "time_us": 1795713348354000,
 "kind": "commit",
 "commit": {"operation": "create", "collection": "app.bsky.feed.post", "rkey": "3n3a6g5ojok2r",
            "cid": "bafyreigx2vwfdxjrdn2eo5ced2u3orw7qp2h2zqtd7sd7eqvxvusuj7gme",
            "record": {"text": "I've fallen down a Reels-hole of watching compilations of NPCs getting volleyballs spiked at them in GTA VI, and...",
                       "langs": ["en"], "createdAt": "2026-11-26T17:15:48.026Z"}}}
```
**Step 2: `bluesky.to_event()`.** `kind=commit`, `operation=create`, `langs` mein `en`, toh pass. `KeywordFilter.match(text)` gives `("gta6", ["gta vi"])`. `event_id = did + "/" + rkey = did:plc:eekr2bqpfbdms77jwngpvfvs/3n3a6g5ojok2r`. `event_ts = time_us / 1e6 = 2026-11-26T17:15:48.354+00:00`.

**Step 3: Kafka message** (topic `raw.bluesky`, key = `event_id` bytes, value = yeh JSON, lz4 compressed, `acks=all`):
```json
{"event_id": "did:plc:eekr2bqpfbdms77jwngpvfvs/3n3a6g5ojok2r", "source": "bluesky", "topic_group": "gta6",
 "event_ts": "2026-11-26T17:15:48.354+00:00", "ingested_ts": "<producer ne us waqt ka time lagaya>",
 "payload": {"text": "I've fallen down a Reels-hole ... GTA VI, and...",
             "url": "https://bsky.app/profile/did:plc:eekr2bqpfbdms77jwngpvfvs/post/3n3a6g5ojok2r",
             "matched_keywords": ["gta vi"],
             "raw": {"did": "...", "rkey": "3n3a6g5ojok2r", "cid": "bafyrei...", "langs": ["en"], "createdAt": "2026-11-26T17:15:48.026Z", "reply": false}}}
```
Key ke hash se **partition 1, offset 70** mila. Producer ka `on_delivery` callback `sent += 1` karta hai.

**Step 4: sink.** Message poll hua, `parse_message("raw.bluesky", 1, 70, value)` ne `Row(source="bluesky", event_id=..., event_time=2026-11-26 17:15:48.354+00, text=..., url=..., matched_keywords=["gta vi"], topic_group="gta6", payload={poora payload}, kafka_topic="raw.bluesky", kafka_partition=1, kafka_offset=70)` banaya. Yeh ek batch mein gaya jo 2026-11-27 11:20:00 UTC (16:50 IST) par insert hua.

**Step 5: `raw.stream_events` row** (Neon se seedha):
```
source=bluesky  event_id=did:plc:eekr2bqpfbdms77jwngpvfvs/3n3a6g5ojok2r
event_time=2026-11-26 17:15:48.354+00  topic_group=gta6  matched_keywords={"gta vi"}
kafka_topic=raw.bluesky  kafka_partition=1  kafka_offset=70
ingested_at=2026-11-27 11:20:00.902634+00   (DB ne default now() lagaya)
```
`event_time` (post kab hui) aur `ingested_at` (hamare DB mein kab aayi) alag hain: yahan ~18 ghante ka fark replay backlog ki wajah se hai.

**Step 6: Kafka commit.** DB commit ke baad batch ka max offset (partition 1 par >= 70) + 1 commit hota hai. Agar yeh post dobara aaye (reset-offsets test) toh `ON CONFLICT (source, event_id) DO NOTHING`: 0 rows insert, `duplicates += 1`.

### At-least-once vs exactly-once
- **At-most-once**: pehle offset commit, phir DB. Crash par message khoya (loss).
- **At-least-once** (hamara): pehle DB commit, phir offset commit. Crash beech mein hua toh message dobara padha jaayega: **duplicate ho sakta hai, loss nahi.**
- **Exactly-once**: har message ka asar ek hi baar. Kafka ke andar (Kafka to Kafka) transactions + `read_committed` se mumkin hai. Par Kafka se **bahari system (Postgres)** mein Kafka transaction DB ke saath atomic nahi hota. Isliye practical tareeqa: **at-least-once + idempotent write** = "effectively once". Hamare case mein idempotency `PRIMARY KEY (source, event_id)` + `ON CONFLICT DO NOTHING` se aati hai. Yeh tabhi kaam karta hai jab event_id stable ho (Bluesky: `did/rkey`, Wikipedia: `meta.id`, HN: item id): replay par same id banta hai.

### Poison message kya hota hai
Aisa message jo **kabhi bhi process nahi ho sakta**, kitni bhi baar retry karo: JSON nahi hai, field missing hai, `topic_group` galat hai, ya DB use reject karta hai (NUL char). Agar consumer sirf crash/retry kare toh woh ek message poori partition ko hamesha ke liye atka deta hai (head-of-line blocking). Hamara tareeqa: **log (topic/partition/offset), count, skip, offset commit.** Transient error (Neon down) alag cheez hai: woh retry hota hai, skip nahi. Prod mein poison ko Dead Letter Queue topic (jaise `raw.dlq`) mein bhejte hain taaki baad mein inspect ho sake; abhi hum sirf log karte hain.

### Chalane ke commands
```powershell
.\scripts\run_stream.ps1 start          # Kafka + 3 producers + db_sink (background, logs\*.log)
.\scripts\run_stream.ps1 status
.\scripts\run_stream.ps1 restart-sink   # graceful stop (stop-file) + start
.\scripts\run_stream.ps1 kill-sink      # hard kill, for crash tests
.\scripts\run_stream.ps1 stop           # sink graceful, baaki sab band; -KeepKafka se Kafka chalta rahe
python -m scripts.counts                # Neon mein source/topic_group counts
python -m scripts.verify_sink           # Kafka vs Neon: loss/duplicate check
python -m streaming.consumers.db_sink --dry-run --limit 100
```

### Jo abhi adhoora hai (honest notes)
- Bluesky producer ne stall ke dauran `failed=1` dikhaya: ek message ka delivery 60s timeout par fail hua. Cursor `flush()` ke baad aage badh jaata hai chahe kuch fail hua ho, toh aise rare case mein ek event producer side par kho sakta hai. Fix: cursor tabhi aage badhe jab `failed` count nahi badha.
- Offset commit group se nikalne ke baad fail hota hai (`UNKNOWN_MEMBER_ID`); sink 3 baar try karke chhod deta hai (rows DB mein hain, replay harmless). Sahi hai, par log noisy hai.
- Neon storage: 6.4k rows abhi chhota hai, par Bluesky backlog replay se tezi se badh sakta hai.

### 5 interview questions (streaming half)
1. **Offsets DB commit ke baad hi kyun commit karte ho?** Agar pehle commit karein aur beech mein crash ho toh message DB mein nahi pahuncha par Kafka ke hisaab se "ho gaya": permanent loss. DB pehle rakhne se worst case duplicate hai, jo unique key rokti hai. At-least-once + idempotent sink.
2. **Exactly-once kyun nahi, aur kab milta hai?** Kafka transactions Kafka-to-Kafka ke liye atomic hain. External DB write aur Kafka offset commit ek atomic unit nahi bante, toh "effectively once" milta hai: at-least-once delivery + idempotent write (`PRIMARY KEY` + `ON CONFLICT DO NOTHING`), ya offsets ko DB mein hi usi transaction mein store karna.
3. **Poison message ko kaise handle karte ho?** Validate karo, log karo (topic/partition/offset), count karo, skip karo aur offset commit karo; crash ya infinite retry nahi (head-of-line blocking). Prod mein DLQ topic. Transient errors (DB down) ko poison nahi maante: retry with backoff, offsets commit nahi.
4. **Consumer group, rebalance aur partitions kya hain? Isse scale kaise hota hai?** Group ke consumers topic ke partitions aapas mein baant lete hain (ek partition ek time par ek hi consumer ko). 3 partitions = max 3 parallel sinks. Consumer zinda rehne ke liye `poll()` karta hai; `session.timeout`/`max.poll.interval` ke andar na kare toh broker use hata ke rebalance karta hai (hamare network stall mein yahi hua, aur replay ke baad duplicates ko DB ne rok liya).
5. **Batch insert 500 rows / 5 sec kyun? Aur Neon down ho toh?** Ek-ek insert par network round trip aur transaction commit ka cost 500 guna hota; batch se throughput badhta hai aur connection kam der khula rehta hai (Neon compute hours). 5 sec ki upper bound se latency control mein. Neon down: `OperationalError` par backoff ke saath retry, offsets commit nahi, Kafka mein data safe (retention 3 din), isliye outage ke baad sink khud catch-up kar leta hai.
