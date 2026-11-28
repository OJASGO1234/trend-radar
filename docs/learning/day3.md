# Day 3 — dbt + Airflow (Hinglish)

## 1. Kya banaya aur kyun
Raw tables (`raw.*`) collectors aur Kafka sink se bharti hain, par unse seedha sawaal poochna mushkil hai: HTML-escaped titles, har video ke alag-alag daily snapshots, events per-row. **dbt** raw se saaf, tested, versioned SQL models banata hai (`analytics` schema mein), aur **Airflow** roz unhe sahi order mein chalata hai: pehle check ki collector ne data diya, phir freshness, phir `dbt build`, phir row counts.

Project: `dbt/` (4 staging views, 5 mart tables, 48 checks). DAG: `airflow/dags/trend_radar_daily.py`. Airflow ek container hai (`infra/docker-compose.airflow.yml`, `mem_limit 1g`, SQLite).

Chalane ke commands:
```powershell
python -m scripts.dbt_cli source freshness     # .env load karke dbt chalata hai (profile DATABASE_URL padhta hai)
python -m scripts.dbt_cli build                # models + tests, dependency order mein
python -m scripts.dbt_cli docs generate        # dbt/target/index.html (python -m scripts.dbt_cli docs serve se dekho)
.\scripts\run_airflow.ps1 up | trigger | status | tasks | password | down
```

## 2. ELT + dbt kyun (ETL nahi)
- **ETL**: data ko load karne se *pehle* transform. **ELT**: pehle **raw ko jaisa hai waisa load** (Extract, Load), phir warehouse ke andar SQL se Transform.
- Hamare yahan collectors aur sink sirf raw likhte hain (`raw.yt_*`, `raw.stream_events`). Fayda: logic galat nikla (jaise `is_short` ki definition) toh raw se dobara bana lo, API dobara hit nahi karni (YouTube quota bachta hai) aur Kafka replay ki zaroorat nahi. Transform ka compute Neon par hota hai, laptop par nahi.
- dbt SQL ko **software** ki tarah treat karta hai: har model ek `.sql` file (sirf `SELECT`), Git mein versioned, tests ke saath, docs auto-generate, aur dependency order khud nikalta hai.

## 3. Staging vs marts
| Layer | Kaam | Hamare models | Materialization |
|---|---|---|---|
| **sources** | raw tables ka naam + freshness rule | `sources.yml` | n/a |
| **staging** (`stg_*`) | **1 raw table = 1 model.** Sirf safai: rename, type, trim, HTML unescape, simple flags. Koi join/aggregation nahi. | `stg_yt_videos`, `stg_yt_video_stats`, `stg_yt_comments`, `stg_stream_events` | **view** (sasta, hamesha latest) |
| **marts** | **Business sawaal ka jawab.** Joins, window functions, aggregations. Dashboard/API seedha inhe padhta hai. | `mart_gta_video_daily`, `mart_top_videos_latest`, `mart_mentions_hourly`, `mart_keyword_daily`, `mart_spikes` | **table** (tez query, `dbt build` par rebuild) |

Kyun alag? Safai ek hi jagah hoti hai (jaise `clean_text` macro), aur 5 marts mein har ek ko wahi safai dobara nahi likhni padti. Staging mein badlav sab marts mein apne-aap pahunchta hai.

Staging mein kya kiya: `clean_text()` macro (`dbt/macros/clean_text.sql`) YouTube ke `&quot; &amp; &#39; &lt; &gt;` ko asli characters banata hai aur whitespace/newlines ko ek space mein. `is_short` = title mein `#shorts` (47 of 239 videos). `is_reply` = Bluesky `payload.raw.reply` (1,819 of 6,384 Bluesky rows replies hain). `event_hour` = `date_trunc('hour', event_time, 'UTC')`.

## 4. `ref()` aur DAG kya hai
- `{{ ref('stg_yt_videos') }}` ek model ko doosre se jodta hai. Do kaam: (1) sahi table ka naam (schema ke saath) bhar deta hai, (2) **dependency graph** banata hai: dbt dekhta hai ki `mart_gta_video_daily` ko `stg_yt_video_stats` aur `stg_yt_videos` chahiye, toh pehle woh banenge. `{{ source('raw','yt_videos') }}` raw table ke liye wahi kaam karta hai (aur freshness check isi se jude hain).
- **DAG = Directed Acyclic Graph**: nodes (kaam) aur arrows (kaun pehle), aur **koi cycle nahi** (A→B→A nahi ho sakta, warna kabhi shuru hi nahi hoga). dbt ka DAG models ka hai; Airflow ka DAG tasks ka hai (`wait_for_collector >> dbt_source_freshness >> dbt_build >> log_mart_row_counts`). Dono ek hi idea.
- Real example: `dbt build` ne 48 nodes (9 models + 39 tests) ko dependency order mein 2 threads par chalaya, 21 sec mein. Test unhi models ke baad chalte hain jinpar woh lage hain, aur model fail ho toh uske downstream tests **SKIP** ho jaate hain (jab `mart_spikes` fail hua tha toh uske 4 tests skip hue).

## 5. Har test kya bachata hai
| Test | Kahan | Kya pakadta hai |
|---|---|---|
| `unique` + `not_null` on keys (`video_id`, `video_day_key`, `event_key`, `*_key`) | staging + marts | Duplicate rows (jaise join ne rows double kar di) ya missing key. Duplicate se counts aur sums galat hote hain bina error ke. |
| `accepted_values` on `source`, `topic_group` | `stg_stream_events`, 3 marts | Naya/typo wala source (`bsky`) ya naya topic group jo dashboards mein kisi group ke andar nahi aayega. DB CHECK constraint raw par hai, par yeh marts tak poora chain protect karta hai. |
| `relationships` stats→videos (aur comments→videos, mart→videos) | staging, marts | **Orphan rows**: stats jinka video raw mein nahi hai (collector ne videos insert nahi kiya ya video delete hua). Join karne par woh rows chupchaap gayab ho jaati. |
| `not_null` on `event_time`, `published_at`, `is_reply`, `is_spike`, `mentions` | staging/marts | Wo columns jinpar time bucketing, spike math aur filters nirbhar hain. NULL aaya toh bucket/flag silently galat. |
| **singular** `assert_no_large_negative_views_gained` | `dbt/tests/` | Views kam nahi hone chahiye. Par YouTube chhote corrections karta hai (spam views hatata hai) aur live streams ke views upar-neeche hote hain. Isliye tolerance: `max(100 views, 0.5% of previous views)`. Isse bada girna = bad data (collector bug, galat video_id, aadha snapshot). Real data mein 5 rows negative hain, sabse bada −3,645 (−0.48%, ek LIVE stream), test pass hota hai, threshold ke bahut paas. |
| source **freshness** | `sources.yml` | Upstream ruka hua hai: `stream_events` (`ingested_at`) warn 2h / error 24h; `yt_video_stats` (`fetched_at`) warn 26h / error 48h. Error par `dbt source freshness` non-zero exit deta hai aur DAG wahin ruk jaata hai, purane data par naye marts nahi banti. |

**Build ke dauran jo galat hua** (honest): (1) pehli run mein `mart_spikes` fail: `trailing` Postgres ka reserved keyword hai (CTE ka naam), `windowed` kar diya, uske 4 tests skip hue the. (2) dbt ne deprecation warn kiya ki `relationships` ke arguments `arguments:` ke andar hone chahiye; theek kiya. (3) Titles mein `&quot;` dikhe: macro bana. Aakhir mein `PASS=48 WARN=0 ERROR=0 SKIP=0`.

## 6. Mart row counts (real)
| Model | Rows | Grain |
|---|---|---|
| `mart_gta_video_daily` | 489 | video × snapshot day |
| `mart_top_videos_latest` | 239 | video (latest snapshot) |
| `mart_mentions_hourly` | 71 | hour × topic_group × source |
| `mart_keyword_daily` | 57 | day × topic_group × keyword |
| `mart_spikes` | 47 | hour × topic_group (har ghanta, quiet wale bhi) |

Staging: 239 videos, 489 stats, 1,074 comments, 6,615 stream events. Top video abhi: "Togi plays GTA6 🎮 #togi" 14.02M views, 222,790 likes, engagement 1.589%. Sabse bada `views_gained` ek din mein ~9.1M ("YOU FINALLY FINISHED GTA 6": pichhle din ke snapshot par ~0.41M views the, ab 9.52M; `days_since_prev=1`). Top keywords: `gta6` 1,288, `gta 6` 1,140, `claude` 886, `gta vi` 656, `anthropic` 488.

Kuch design choices jo SQL padhte waqt samajh aani chahiye:
- `views_gained` = `LAG(views) over (partition by video_id order by snapshot_date)`. Pehle snapshot par NULL (peeche kuch nahi). `days_since_prev` batata hai ki beech mein collector ka din miss hua ya nahi (>1).
- `engagement_rate = likes / nullif(views, 0)`: zero se divide nahi, aur jahan likes chhupe hain (NULL), rate bhi NULL (jaise "YOU FINALLY FINISHED GTA 6" ke likes hidden hain).
- `mart_top_videos_latest`: `DISTINCT ON (video_id) ... ORDER BY video_id, snapshot_date DESC` har video ki sirf latest row rakhta hai (Postgres ka shortcut), phir `rank() over (order by views desc)`.
- `mart_keyword_daily`: `matched_keywords` ek array hai, `unnest()` ek event ko har keyword ke liye ek row bana deta hai.

## 7. Z-score spike detector: kaise kaam karta hai
Har `topic_group` ke har **ghante** ke liye hum poochte hain: "pichhle 24 ghanton ke mukable yeh ghanta kitna unusual hai?"
```
z = (is ghante ki mentions − pichhle 24 ghanton ka mean) / unka standard deviation
is_spike = z >= 3 AND mentions >= 10 AND history_hours >= 6
```
- z = "normal se kitne standard deviations upar". 3σ ka matlab normal (bell-curve jaisa) data mein ~0.1% ghante hi itne upar hote hain, isliye rare = spike.
- **Window**: `rows between 24 preceding and 1 preceding`, yaani current ghanta **shamil nahi**, warna spike apne hi average ko upar kheench ke z chhota kar deta.
- **Spine (generate_series)**: pehle har group ke first se last ghante tak har ghanta banate hain aur quiet ghanton ko `0` maante hain. Warna bina mentions wale ghante table se gayab hote aur "normal" jhoothmooth upar dikhta.
- **`mentions >= 10` floor**: 0 → 3 mentions ka z bahut bada ho sakta hai par woh news nahi. Absolute floor tiny numbers ko fire hone se rokta hai.
- **`history_hours >= 6` guard (maine joda, spec mein nahi tha)**: mean/stddev 1-2 points se nikalna bekaar hai. Real data ne iski zaroorat dikha di:

Real example 1, **guard ke bina galat spike**: `gta6`, 2026-11-26 16:00 UTC: mentions = 37, trailing mean = 2.50, stddev = 0.71, history sirf **2** ghante. `z = (37 − 2.50) / 0.707 = 48.79`. z ≥ 3 aur count ≥ 10 dono pass, par yeh spike nahi tha: pipeline abhi shuru hua tha (Bluesky ke backlog replay ka pehla ghanta), peeche sirf 2 data points. Guard ne ise `is_spike = false` rakha.

Real example 2, **normal ghanta**: `gta6`, 2026-11-27 10:00 UTC: mentions = 14, mean = 166.40, stddev = 85.27, history = 20 ghante. `z = (14 − 166.40) / 85.27 = −1.79`. Negative z = normal se **kam** (replay ka speed ghata aur producers band the), spike nahi.

**Nateeja**: 47 rows mein `is_spike = true` **0** hain. Yeh sahi hai: hamara stream data ek 20 ghante ka backlog replay hai jo lagbhag ~200/hr ki flat raftaar se aaya, usme koi asli spike hai hi nahi. Detector tabhi dikhayega jab live data aur GTA 6 ka koi trailer/news aayega. (Ek bada caveat: trailing stddev replay-heavy data par bada hai, jo spike ko chhupa sakta hai; jab 24+ ghante ka live data jama ho jaye tab thresholds dobara dekhna.)

## 8. Airflow
### Airflow vs cron
Cron sirf "is time par yeh command chalao". Airflow add karta hai: **dependencies** (B tabhi jab A safal), **retries + delay** (`retries=2`, 5 min), **sensors** (kisi condition ka intezaar), **UI** (har run/har task ka status, logs, manual re-run), **backfill** (purane dino ke liye chalao, `logical_date` ke saath), aur history. Cron mein har step alag line hoti hai: agar collector late hua toh cron phir bhi purane data par dbt chala deta, aur kisi ko pata nahi chalta.

### Sensor kya hai
Ek task jo kaam nahi karta, **kisi condition ke sach hone ka intezaar** karta hai. Hamara `wait_for_collector` (`PythonSensor`) har 10 min mein `raw.collector_runs` mein dekhta hai ki us UTC din ka `status='success'` hai ya nahi; 2 ghante tak nahi mila toh fail. `mode="reschedule"`: har check ke baad task worker slot chhod deta hai aur `up_for_reschedule` state mein soya rehta hai. (Dusra mode `poke` slot pakde rehta hai; hamare SequentialExecutor mein woh baaki sab rok deta.)

**Real dekha**: DAG unpause karte hi `scheduled__2026-11-27T01:30` run chala (jo 01:30 UTC ka tick tha, aur abhi UTC mein Nov 28 hai). Sensor log: `collector successes since 2026-11-28 00:00:00+00:00: 0 (latest None)` aur phir `Rescheduling task, marking task as UP_FOR_RESCHEDULE`; dbt tasks `None` (shuru hi nahi hue). Yani bina collector ke dbt nahi chala, jo hona chahiye.

### Collector GitHub Actions par hi kyun, aur Airflow sirf transform kyun
- **GitHub Actions collector ka scheduler rahega**: woh laptop band hone par bhi chalta hai (GitHub ke servers par). Airflow tumhare laptop ke Docker mein hai; laptop band toh Airflow band, toh collector miss hota.
- Airflow ko collector chalane ke liye bhi lagana duplicate hota: do jagah scheduling, do jagah secrets (`YOUTUBE_API_KEY` container mein bhi), aur dono ek hi din do baar chalein toh quota double. Isliye Airflow sirf **collector ka result dekhta hai** (sensor, DB mein `collector_runs` se) aur transform side orchestrate karta hai (jahan dependencies, tests aur retries ki sach mein zaroorat hai). Dono ke beech ka contract: `raw.collector_runs` table.

### DAG ke tasks
`trend_radar_daily`, schedule `0 10 * * *` (10:00 UTC; Day 4 mein 01:30 se badla, neeche section 9 dekho), `catchup=False`, `max_active_runs=1`, `retries=2`, `retry_delay=5 min`:
1. `wait_for_collector` (sensor, timeout 2h)
2. `dbt_source_freshness`: `dbt source freshness`. Error ho toh task fail; warn par pass.
3. `dbt_build`: `dbt build` (models + tests).
4. `log_mart_row_counts`: Python task, 5 marts ki row counts log karta hai aur kisi mart ke **0 rows** hone par fail.

dbt Airflow image ke andar **alag virtualenv** (`/home/airflow/dbt-venv`) mein hai, taaki Airflow ke pinned packages se na ladein. Repo `:ro` mount hai, isliye dbt ka `target/` aur `logs/` `/tmp` mein jaate hain (`DBT_TARGET_PATH`, `DBT_LOG_PATH`). `DATABASE_URL` hi akela secret container mein jaata hai (`.env` poora nahi, aur `.dockerignore` build context se `.env` ko bahar rakhta hai).

### Ek trigger, har task ka status (real)
Maine sensor wali stuck scheduled run ko failed mark kiya (`max_active_runs=1` ki wajah se doosri run uske peeche queue mein thi), phir manual run (`-e 2026-11-27T12:00:00+00:00`, jiske "aaj" mein collector ka 08:52 UTC success hai) chali:

| Task | State | Time (UTC) | Kya hua |
|---|---|---|---|
| `wait_for_collector` | **success** | 02:08:25 → 02:08:27 | `collector successes since 2026-11-27 00:00:00+00:00: 1 (latest 2026-11-27 08:52:15)` |
| `dbt_source_freshness` | **success** | 02:08:31 → 02:08:49 | `PASS yt_video_stats`, **`WARN stream_events`** (13 ghante purana, warn 2h par, error 24h par, isliye fail nahi) |
| `dbt_build` | **success** | 02:08:54 → 02:09:24 | `PASS=48 WARN=0 ERROR=0 SKIP=0` (5 table models, 39 tests, 4 views) |
| `log_mart_row_counts` | **success** | 02:09:29 → 02:09:30 | 489 / 239 / 71 / 57 / 47 (upar ki table) |

Run total ~1 min 5 sec. **Memory**: container startup par peak ~904 MiB (limit 1 GiB), dbt chalte waqt ~706 MiB, idle ~375-412 MiB. 1 GiB limit mein fit hota hai par margin patla hai: Airflow standalone ke saath Kafka/monitoring stack **ek saath mat chalao**.

## 9. Do cheezein jo tumhe pata honi chahiye (honest findings)

> **Update (Day 4): dono fix ho gaye.** DAG ab `0 10 * * *` (10:00 UTC) par chalta hai (GitHub ka collector ~08:40-08:55 UTC par chalta hai, sensor 2h ka rakha). Aur `stream_events` freshness ab **sirf warn** hai (`error_after` hata diya), toh band stream DAG ko kabhi block nahi karega; `yt_video_stats` freshness abhi bhi blocking (error 48h). Neeche ka original analysis history ke liye rakha hai.
1. **GitHub Actions ka "00:30 UTC" cron asal mein ~08:40-08:52 UTC par chal raha hai**: `gh run list` mein scheduled runs `2026-11-26T08:40Z` aur `2026-11-27T08:51Z` par shuru hue, yaani ~8 ghante late (GitHub scheduled workflows busy time par delay hote hain aur guarantee nahi dete). Isliye 01:30 UTC wale DAG ka sensor 2 ghante (till ~03:30 UTC) mein collector nahi dekhega aur **roz fail hoga**. Yeh spec ke hisaab se banaya hai, par sudharna padega: (a) DAG ko ~10:00 UTC par schedule karo (`schedule="0 10 * * *"`), ya (b) sensor `timeout` ~10 ghante karo, ya (c) collector ko GitHub ki jagah dusre reliable trigger se chalao. Mera suggestion (a) + sensor timeout 2h. Mujhe batao toh badal doon.
2. `dbt source freshness` ka error 24h: jab streaming stack band rehta hai (laptop par aksar), `stream_events` 24h baad **error** ho jaayega aur DAG wahin ruk jaayega (YouTube marts bhi nahi banengi). Option: stream ke liye error_after ya DAG mein freshness ko non-blocking karna.

## 10. 5 interview questions
1. **ETL vs ELT, aur dbt kahan fit hota hai?** ETL mein load se pehle transform; ELT mein raw pehle load, transform warehouse ke SQL se. dbt "T" ka tool hai: SELECT models, dependency graph (`ref`), tests, docs. Fayda: raw immutable rehta hai, logic badle toh rebuild, source API dobara hit nahi.
2. **Staging vs marts?** Staging = 1:1 raw ki safai (rename/cast/trim), views. Marts = business logic (join, window, aggregate), tables, jo consumers padhte hain. Safai ek jagah, logic ek jagah.
3. **dbt tests kis cheez ke liye? Ek example do.** Data quality contracts: unique/not_null keys, accepted_values, relationships (referential integrity), aur singular tests (custom SQL, row return hui toh fail). Example: `assert_no_large_negative_views_gained` bad snapshot pakadta hai, par YouTube ke chhote corrections ko tolerance deta hai.
4. **Z-score spike detection kaise? Limitations?** `z = (x − μ) / σ` pichhle 24h window se (current ghanta chhod ke), `z>=3` aur count floor, aur gap-fill (quiet hours = 0). Limitations: bell-curve assume karta hai (counts skewed hote hain), trending data mein σ bada hota hai, seasonality (raat/din) nahi pakadta, aur kam history par unreliable (isliye ≥6 ghante ka guard).
5. **Airflow vs cron, aur sensor kya hai?** Airflow dependencies, retries, backfill, UI aur observability deta hai; cron sirf time par command. Sensor ek task hai jo condition (file, DB row, upstream DAG) ka intezaar karta hai; `reschedule` mode worker slot chhod deta hai. Hamare yahan sensor collector ke success ka intezaar karta hai taaki dbt purane data par na chale.
