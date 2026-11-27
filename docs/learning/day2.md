# Day 2 — Monitoring: Prometheus + Grafana (Hinglish)

## 0. Pehle do fixes (Day 1 ke baad)
1. **CLAUDE.md**: conflict target `(source, event_id)` kar diya (primary key yahi hai). Pehle `event_id` likha tha, jo Postgres `ON CONFLICT` mein reject karta.
2. **Producers ka cursor ab sirf tab aage badhta hai jab koi delivery fail nahi hui.** Pehle `flush() == 0` kaafi maana jaata tha, par `flush()` tab bhi 0 deta hai jab message *fail* hokar queue se nikal gaya ho. Ab `Runtime.checkpoint_ok()` (`streaming/common/runtime.py`) do cheezein dekhta hai: (a) flush ke baad kuch unsent nahi bacha, (b) `failed` count last checkpoint ke baad badha nahi. Agar failed badha, toh state file **us run ke baaki hisse mein roki rehti hai**, aur next run usi purani position se replay karta hai (duplicates DB rok leta hai, gap nahi banta). Teeno producers (`bluesky`, `wikipedia`, `hackernews`) ab `rt.checkpoint_ok()` use karte hain.
   Proof: `python -m unittest tests.test_checkpoint -v` → 4 tests pass (clean run state badhata hai, unsent block karta hai, ek failure ke baad state *hamesha ke liye* held rehta hai chahe baad ke events theek jaayein, aur state file ka purana content overwrite nahi hota). Stub producer use kiya, Kafka ki zaroorat nahi.

## 1. Kya banaya aur kyun
Pipeline chal raha hai, par "chal raha hai" ka pata kaise chalega? Kal Day 1 mein laptop ~2 min atak gaya aur humne sirf log padhkar samjha. Ab har process apna health `/metrics` par expose karta hai, **Prometheus** har 15 sec mein unhe scrape karke time-series DB mein rakhta hai, aur **Grafana** unse dashboard banata hai. Do alert bhi hain: `SinkDown` aur `HighLag`. Sab kuch Docker mein (Prometheus 300 MB, Grafana 250 MB limit), producers/sink Windows par chalte hain.

Ports: bluesky `8001`, wikipedia `8002`, hackernews `8003`, db_sink `8004` (`.env` mein `METRICS_PORT_*` se badal sakte ho). Prometheus `9090`, Grafana `3000`.

## 2. Prometheus ka pull model
- **Push** (jaise StatsD): app metrics server ko bhejta hai. **Pull** (Prometheus): app sirf ek HTTP page `/metrics` rakhta hai (plain text), aur Prometheus khud jaake padhta hai.
- Fayde: (1) **`up` metric free**: scrape fail hua toh `up == 0`, yaani process down ka pata bina app ke kuch bheje chal jaata hai. Isi se `SinkDown` alert bana. (2) App ko Prometheus ka address/credentials nahi chahiye; Prometheus down ho toh app par asar nahi. (3) Khud browser mein `http://localhost:8004/metrics` khol ke debug kar sakte ho.
- Nuksaan: short-lived jobs (jo scrape se pehle khatam ho jaayein) ke liye Pushgateway chahiye. Hamare producers/sink long-running hain, isliye pull perfect hai.
- Scrape har 15 sec: matlab dashboard ki resolution 15 sec hai. Isse chhota koi spike dikh nahi sakta.
- Prometheus Docker mein hai aur targets Windows host par, isliye `host.docker.internal:800X`. Isi wajah se metrics server `0.0.0.0` par bind hota hai (127.0.0.1 par Docker ke andar se nahi pahunchta). Matlab same LAN par koi bhi `/metrics` padh sakta hai; usme secret nahi hai, par cafe ke WiFi par Windows Firewall prompt "Public" ke liye Deny kar dena.

## 3. Counter vs Gauge vs Histogram
| Type | Kya hai | Hamare example |
|---|---|---|
| **Counter** | Sirf badhta hai (restart par 0). Kabhi seedha value mat dekho, `rate()`/`increase()` lagao. | `events_seen_total`, `events_kept_total`, `events_sent_total`, `events_failed_total`, `rows_inserted_total`, `rows_duplicate_total`, `poison_total`, `messages_consumed_total` |
| **Gauge** | Upar-neeche ho sakta hai: abhi ki value. | `consumer_lag{topic,partition}`, `last_commit_timestamp_seconds` |
| **Histogram** | Values ko **buckets** mein ginta hai (`_bucket{le="5"}` = kitne observations ≤ 5 sec), plus `_sum` aur `_count`. Isse percentiles nikalte hain. | `event_latency_seconds`, `batch_insert_seconds` |

Format ka namuna (`/metrics` ka text):
```
# TYPE events_kept_total counter
events_kept_total{source="bluesky"} 412.0
# TYPE consumer_lag gauge
consumer_lag{topic="raw.bluesky",partition="1"} 0.0
# TYPE batch_insert_seconds histogram
batch_insert_seconds_bucket{le="1.0"} 7.0
batch_insert_seconds_bucket{le="2.5"} 31.0
batch_insert_seconds_bucket{le="+Inf"} 33.0
batch_insert_seconds_count 33.0
```
(Yeh format dikhane ke liye hai; asli numbers neeche panels mein.) Buckets **cumulative** hote hain: `le="2.5"` mein woh bhi aate hain jo `le="1.0"` mein the.

## 4. `rate()` counter par kyun?
Counter ki raw value (jaise `events_kept_total = 4530`) kuch nahi batati: woh process shuru hone se ab tak ka total hai, restart par 0 ho jaata hai. Humein chahiye "abhi kitne per second". `rate(events_kept_total[1m])` pichle 1 minute ke samples ka per-second slope nikalta hai, aur **counter reset (restart) ko khud sambhalta hai** (value girti dikhe toh maan leta hai ki restart hua). `increase(x[5m])` = 5 min mein kitna badha. Gauge par `rate()` mat lagana, woh badhta-ghatta hai; counter par raw value mat plot karna.
Window (`[1m]`) kam se kam 4 scrape intervals (4 × 15s) ki rakho, warna rate noisy ya khaali aata hai.

## 5. p95 ka matlab, aur average kyun nahi
**p95 = 95% observations is value se tez ya barabar hain, 5% isse dheere.** p50 = median. Average outliers se bigad jaata hai: 99 events 0.2 sec mein aur 1 event 100 sec mein, average ~1.2 sec aayega par woh kisi event ka asli experience nahi. p95/p99 "tail" dikhate hain, jo users ya downstream ko asal mein khatakta hai.
`histogram_quantile(0.95, sum by (le, source) (rate(event_latency_seconds_bucket[5m])))`: pehle buckets ka rate, phir `le` ke hisaab se jod ke, phir bucket ke andar **linear interpolation** karke estimate. Matlab percentile utna hi sahi jitne buckets patle hain. Hamare buckets 0.5 s se 24 ghante tak hain, isliye bade latency (replay) ka sirf andaza milta hai (neeche dekho).

## 6. Consumer lag: streaming ka sabse zaroori metric
`lag = high watermark (topic mein sabse naya offset) − committed offset (jahan tak sink ne kaam khatam kiya)` per partition. Yeh seedha batata hai ki **hum kitna peeche hain**:
- Lag 0 ya chhota aur stable: sink producers ke saath chal raha hai.
- Lag badh raha hai: sink dheema hai ya Neon par atka hai ya crash hua. Throughput (msgs/sec) normal dikh sakta hai tab bhi, par lag batata hai ki data user tak late pahunch raha hai.
- Lag sirf "kitna kaam baaki hai" nahi, data freshness hai: Kafka mein retention (3 din) se zyada lag hua toh messages delete ho jaayenge = permanent loss.
Hamare sink mein lag ek **alag probe consumer** (thread) se nikalta hai (`_lag_loop` in `db_sink.py`) jo group join nahi karta (sirf `committed()` aur `get_watermark_offsets()` call karta hai), isliye rebalance nahi hota, aur Neon retry mein atka hone par bhi measure hota rehta hai.

## 7. File-by-file walkthrough
- `streaming/common/metrics.py`: `ProducerMetrics` (4 counters with `source` label + latency histogram) aur `SinkMetrics`. Har object ka **apna `CollectorRegistry`** hai, taaki ek hi process mein kai objects (tests) `DuplicateTimeseries` error na dein. `serve()` HTTP server thread start karta hai; port busy ho toh sirf log karta hai, pipeline nahi girta (monitoring pipeline se zyada important nahi).
- `streaming/common/config.py`: `METRICS_ADDR` aur `METRICS_PORT_*` env se.
- `streaming/common/runtime.py`: `seen()`/`emit()` counters badhate hain; `emit()` mein latency = `ingested_ts − event_ts` observe hota hai (negative ho toh 0, clock skew se). `--dry-run` mein server start nahi hota (asli producer ke saath port ki ladai nahi).
- `streaming/common/kafka_helper.py`: `sent`/`failed` counters Kafka ke **delivery callback** mein badhte hain, yaani broker confirm karne par, `produce()` call par nahi.
- `streaming/consumers/db_sink.py`: `messages_consumed_total{topic}`, `poison_total{topic}`, `rows_inserted_total`, `rows_duplicate_total`, `batch_insert_seconds` (connect + INSERT + COMMIT), `consumer_lag{topic,partition}`, `last_commit_timestamp_seconds` (sirf offset commit **safal** hone par set).
- `infra/docker-compose.monitoring.yml`: Prometheus `mem_limit 300m`, `--storage.tsdb.retention.time=3d`; Grafana `mem_limit 250m`, datasource aur dashboard **provisioned** (file se, UI mein haath se nahi), taaki dashboard Git mein versioned rahe aur `docker compose down -v` ke baad bhi wapas aa jaye.
- `infra/prometheus/prometheus.yml`: 2 jobs (`producers` ke 3 targets with `source` label, `db_sink`), 15s scrape. `infra/prometheus/alerts.yml`: `SinkDown` (`up{job="db_sink"} == 0` for 1m), `HighLag` (`sum(consumer_lag) > 1000` for 5m). `for:` ka matlab condition lagatar itni der sach rahe tab hi fire, taaki ek scrape ki hiccup par alert na bajey.
- `infra/grafana/dashboards/trend-radar.json` (12 panels), `infra/grafana/provisioning/*`.
- `scripts/run_stream.ps1`: `start` ab Kafka + monitoring + 4 processes uthata hai; `stop` sab band (`-KeepKafka`, `-KeepMonitoring` se rakh sakte ho).
- `scripts/panel_report.py`: dashboard JSON se har panel ki queries Grafana ke datasource proxy se chalata hai aur last/min/max chhapta hai (neeche ke numbers isi se).

## 8. Real run: har panel, asli numbers
Setup: 3 producers + sink, 18:25:39 se, 15 sec scrape, window = last 5 min (~18:26 se 18:31 IST). Prometheus par 4 targets `up`. Grafana ne datasource (`Prometheus → http://prometheus:9090`) aur dashboard (12 panels, folder "Trend Radar") khud load kiye.

| # | Panel | Asli numbers (5 min) | Matlab |
|---|---|---|---|
| 1 | **Targets up** | 4 (min 4, max 4) | Chaaro scrapes safal: 3 producers + sink |
| 2 | **Total consumer lag** | abhi 1, max 22 | Sink lagbhag caught up; max 22 messages ek batch ke beech ka tha |
| 3 | **Seconds since last offset commit** | 13.4 s (range 12.7–21.6) | Har ~5-10s batch commit ho raha hai. Pehle yeh 1.79 billion sec dikha raha tha (gauge ki shuruaati value 0 thi, `time() - 0`). Query mein `and last_commit_timestamp_seconds > 0` lagaya |
| 4 | **Rows inserted** (range) | ~105 rows | Neon mein 5 min mein naye rows |
| 5 | **Kept events/sec per source** | bluesky 0.156/s (0.027–0.289), wikipedia 0.067/s (0.022–0.222), hackernews 0.000/s (peak 0.818 jab 60s poll mein items aaye) | Filter ke baad kitna bacha |
| 6 | **Kept ratio (kept/seen)** | bluesky 0.28%, hackernews 7.4%, wikipedia 0.007% | Firehose ka kitna % hamare keywords wala hai. Bluesky ka ~0.2-0.3% Day 1 ke number se match karta hai. Wikipedia sirf titles par match hota hai isliye lagbhag 0 |
| 7 | **Latency p50 / p95** (source → producer) | bluesky p50 0.26 s, p95 0.49 s; hackernews p50 416 s, p95 3,139 s; wikipedia p50 ~54,000 s, p95 ~83,000 s | Neeche dekho |
| 8 | **Lag per topic** | raw.bluesky 1 (max 3), raw.hackernews 0 (max 18), raw.wikipedia 0 (max 3) | Teeno topics par sink saath chal raha hai |
| 9 | **Inserted vs duplicates vs poison** | inserted 0.378/s (peak 0.934), duplicates 0/s, poison: koi sample nahi | Poison ki series tab banti hai jab pehli baar kuch ho; is run mein 0 |
| 10 | **Failed deliveries (5m)** | teeno sources 0 | Cursor hold-back ka koi trigger nahi hua |
| 11 | **Batch insert time p50 / p95** | p50 1.54 s, p95 2.40 s | Neon ko connect + INSERT + COMMIT mein ~1.5 s: zyadatar network round trips (free tier, India se). Chhote batch (~20 rows) ke liye yeh bada hai, isliye 5-sec wait theek hai |
| 12 | **Sink throughput msgs/sec** | bluesky 0.200/s, wikipedia 0.156/s, hackernews 0/s (peak 0.619) | Panel 5 ke saath milta hai: lag ~0 hone par consumed ≈ kept |

**Latency ka sach (panel 7)**: Bluesky live hai (cursor ki file nahi thi, toh "ab se" shuru), isliye 0.26 s median. Wikipedia aur HN **purane backlog se catch-up kar rahe the**: Wikipedia ka saved `Last-Event-ID` ~9-10 ghante peeche ka tha (Day 1 mein bhi replay chal raha tha jab humne use band kiya), aur HN ne downtime ke dauran aaye items utha liye. `event_ts` item ke asli time ka hai, `ingested_ts` abhi ka, isliye replay latency ghanton mein dikhti hai. Yeh bug nahi, yeh metric ki **semantics** hai: "event kitna purana hai jab hum use dekhte hain". Isliye dashboard dekh ke backlog replay aur live lag mein farq pehchaanna seekho. Aur Wikipedia ke 54,000/83,000 sirf bucket ke andar interpolation hain (6h se 24h wale bucket mein sab kuch tha), asli value ek range hai.
Yeh latency **source → hamara producer** ka hai (spec ke mutabik `ingested_ts − event_ts`). Kafka ke through DB tak ka lag alag panel (consumer lag) dikhata hai.

### Docker stats (monitoring + Kafka chalte hue)
```
NAME         CPU %     MEM USAGE / LIMIT   MEM %
grafana      0.09%     132.6MiB / 250MiB   53.06%
prometheus   0.57%     122.9MiB / 300MiB   40.97%
kafka        1.97%     461MiB / 900MiB     51.23%
```
Teeno containers milke ~717 MiB, jabki Docker ko 3 GB mile hain. Python processes Windows par alag chalte hain.

### Alert test (SinkDown)
18:31:59 par sink ko `taskkill /F` se maara. 18:33:50 par Prometheus: `up{job="db_sink"} = 0` aur alert `SinkDown` **firing** (1 minute `for:` ke baad). Restart ke 50 sec baad active alerts: 0, lag 2. Honest note: maine sink ke chalte hue 1000+ lag banakar `HighLag` fire hote nahi dekha (is run mein lag ka max 26 tha), woh rule sirf load hua dikhta hai (`state=inactive`).

**Ek design limitation jo test mein dikhi**: `consumer_lag` gauge sink process ke andar hai. Sink mara toh uski series **stale ho gayi aur graph par lag badhta nahi dikha**, series gayab ho gayi. Isliye sink-down ko `SinkDown` pakadta hai, `HighLag` nahi. Production mein lag ko alag exporter se nikaalte hain (jaise `kafka-lag-exporter` ya Burrow), jo consumer se independent chalta hai. Abhi naya tool nahi joda (CLAUDE.md ke hisaab se pehle poochna padta).

## 9. Chalane ke commands
```powershell
.\scripts\run_stream.ps1 start      # Kafka + Prometheus + Grafana + 3 producers + sink
# Grafana:    http://localhost:3000  (admin / admin; anonymous viewer bhi on hai, sirf local)
# Prometheus: http://localhost:9090  (Status > Targets, Alerts)
python -m scripts.panel_report --minutes 5    # har panel ke numbers terminal mein
.\scripts\run_stream.ps1 stop
```

## 10. 5 interview questions
1. **Prometheus pull model kya hai aur push se behtar kab?** Prometheus har target ka `/metrics` khud scrape karta hai. Fayde: `up` metric se down detection free, apps ko monitoring server ka pata nahi chahiye, service discovery ke saath scale. Push (Pushgateway) sirf short-lived batch jobs ke liye.
2. **Counter, gauge, histogram mein kya farq? Counter par `rate()` kyun?** Counter sirf badhta hai (total), gauge upar-neeche (current value), histogram buckets mein distribution. Counter ki raw value restart par 0 hoti hai aur sirf cumulative hai; `rate()` per-second slope deta hai aur counter reset khud handle karta hai.
3. **p95 latency kya hai, average kyun nahi? Histogram se kaise nikalte ho?** 95% requests is se tez. Average outliers se chhup jaata hai. `histogram_quantile(0.95, sum by (le) (rate(x_bucket[5m])))`, bucket ke andar linear interpolation, isliye accuracy bucket boundaries par depend karti hai.
4. **Consumer lag kya hai aur isse alert kyun?** High watermark − committed offset. Yeh sink kitna peeche hai batata hai (data freshness). Badhta lag = consumer dheema/atka; retention se zyada hua toh data loss. Alert `sum(lag) > threshold for 5m` (`for` se transient spikes ignore).
5. **`SinkDown` alert kaise bana aur uski limitation kya hai?** `up{job="db_sink"} == 0 for 1m`, `up` Prometheus khud har scrape par banata hai. Limitation: agar lag metric usi process se nikalta hai jo mar gaya, toh lag series bhi gayab ho jaati hai, isliye lag ke liye independent exporter chahiye, aur "process zinda par stuck" ke liye `last_commit_timestamp` ka age alert achha hai.
