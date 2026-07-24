# migkit

> Verify, repair, ขน database ข้าม engine — โดยไม่ต้องเชื่อตัวขน

[![engines](https://img.shields.io/badge/engines-postgres%20·%20mysql%20·%20mongodb%20·%20mssql%20·%20sqlite%20·%20redis%20·%20kafka-2a78d6)](#3-คำสั่งทั้งหมด)
[![cross-engine](https://img.shields.io/badge/cross--engine-mysql→postgres-0ca30c)](#)
[![python](https://img.shields.io/badge/python-3.10+-3776ab)](pyproject.toml)
[![status](https://img.shields.io/badge/status-active-0ca30c)](CHANGELOG.md)

[English README →](README.md) · [Changelog →](CHANGELOG.md)

---

เครื่องมือ migrate database ส่วนตัว: เตรียมปลายทาง, validate ให้ตรง 100%,
ซ่อมส่วนที่ต่าง, และบอกจังหวะว่าเมื่อไหร่ต้องกดอะไรใน migration service

หลักคิดสำคัญ: **migkit ไม่ขน data เอง** — งานขน data เป็นของ managed service
(managed migration service ของ cloud หรือ native tools) หรือ native tools ที่วิ่งบน network ของ cloud
ซึ่งเสถียรกว่าเน็ตเรา migkit ทำทุกอย่างรอบๆ การขน:

```
เตรียม schema ปลายทาง -> บอกจังหวะเปิดตัวขน -> เฝ้าดูระหว่างขน
-> validate ครบทุกชั้น -> ซ่อมส่วนที่ mover ขนมาไม่ได้ (เช่น sequence)
```

รองรับ 4 ระดับ:
- **native**: postgres, mysql, mssql, mongodb, sqlite, redis, kafka
- **alias ตระกูลเดียวกัน**: mariadb, percona, tdsql, aurora-mysql,
  aurora-postgres, alloydb, documentdb, cosmosdb-mongo, azure-sql
- **generic ผ่าน reladiff**: snowflake, bigquery, redshift, clickhouse,
  oracle, trino, presto, duckdb, vertica, databricks
- **schema ผ่าน liquibase (JDBC)**: db2, h2, firebird, informix, sybase ฯลฯ
  แค่วาง driver jar
- **ข้าม engine (hetero)**: mysql -> postgres ครบสาย: `schema --convert`
  (sqlglot/pgloader) -> `move` (resumable) -> `move --mode cdc` (CDC จาก
  binlog พร้อม checkpoint ตำแหน่ง) -> validate ด้วย reladiff ข้าม dialect

ตั้งกี่ hop ก็ได้ (hop = ขาการย้าย 1 เส้น) engine ใหม่ = implement
interface เดียว ~150 บรรทัด

ตัว package จบในตัวเอง ไม่พึ่งไฟล์นอกโปรเจกต์: script ตรวจ postgres ฝังอยู่ใน
`migkit/scripts/pg/` ผลลัพธ์ทั้งหมดลง `reports/` และ override ได้ด้วย env:
`MIGKIT_CONF` (ที่อยู่ hops.yaml), `MIGKIT_REPORTS` (ที่เก็บผล),
`PG_DIFF_CHECKER` (ชี้ script ชุดอื่นตอน dev)

---

## 1. ติดตั้ง

```bash
cd devops-tools/migkit
./bootstrap.sh
source .venv/bin/activate
```

bootstrap ทำให้ครบ: ลง libpq (psql/pg_dump), สร้าง python venv หลัก,
ลง lib หลัก + optional (pymysql, pymongo, redis, kafka, migra)
และสร้าง `.venv-tools` (python 3.12 ผ่าน uv) สำหรับ reladiff แยกต่างหาก
เพราะ reladiff ยังไม่รองรับ python 3.14 — migkit หา tools จากทั้งสอง venv
ให้อัตโนมัติ ตัวไหนลงไม่ได้จะข้ามแล้วบอก ทุก feature มี fallback ในตัว

tools ภายนอกที่ลงไว้แล้วบนเครื่องนี้: mysql-client (mysqldump), sqlcmd,
percona-toolkit (pt-table-sync), mongosh + mongodb-database-tools
(mongodump/mongorestore), liquibase, reladiff, migra

เช็คว่าพร้อม:

```bash
migkit doctor
```

จะโชว์ 2 ส่วน: tools บนเครื่อง (อันไหน missing) และยิง connection จริงไปทุก hop
ทั้งฝั่ง src และ dst ถ้าเห็น `ok (host, N dbs)` คือใช้ได้

---

## 2. ตั้งค่า hop (conf/hops.yaml)

ไฟล์นี้ gitignore ไว้เพราะมีรหัสผ่าน (chmod 600 ให้อัตโนมัติ)
เริ่มจาก copy `conf/hops.example.yaml`

```yaml
hops:
  mig-a:            # ตั้งชื่ออะไรก็ได้ ใช้ชื่อนี้เรียกทุกคำสั่ง
    engine: postgres              # postgres | mysql | mssql | mongodb | redis | kafka
    service: native               # ชุด playbook: aws-dms | tencent-dts | gcp-dms | native
    source:
      host: source-db.example.com
      port: 5432                  # ไม่ใส่ = default ของ engine
      user: postgres
      password: "secret"
    target:
      host: 10.0.0.10
      user: root
      password: "secret"
    databases:                    # ไม่ใส่/ว่าง = discover จาก source อัตโนมัติ
      - appdb
      - orders
    workers: 4                    # เช็คกี่ database พร้อมกัน
    big_rows: 5000000             # เกินนี้ใช้โหมด slice (postgres)
    slice: 1000000                # ขนาด slice ต่อรอบ
```

ข้อควรระวังเรื่อง scope: ถ้า source มี db ปนกันหลาย environment (เช่น RDS มี
dev+uat) ให้ระบุ `databases` เองเสมอ อย่าปล่อย auto ไม่งั้นจะไปเทียบ db
ที่อีกฝั่งไม่มี

---

## 3. คำสั่งทั้งหมด

ตั้งแต่ 0.2.0 เหลือ 11 คำสั่งหลัก — ชื่อเดิมทุกตัวยังใช้ได้ (เป็น hidden alias
พร้อม flag เดิมครบ) script เก่าไม่พัง:

| ชื่อเดิม | ตอนนี้คือ |
|---|---|
| `hops` | `doctor` (โชว์ตาราง hop ให้ด้วย) |
| `setup-target` | `schema` |
| `convert-schema` | `schema --convert` |
| `gen-migration` | `schema --migration` |
| `sample-diff` | `check --drill` |
| `repair` | `sync` (dry-run) / `sync --apply` |
| `replicate` / `tail` | `move --mode cdc` หรือ `--mode full+cdc` |
| `monitor` | `watch --verify` |
| `state` | `history` |
| `ui` | `report --serve` |

เติม `-q` หน้าคำสั่งไหนก็ได้ = quiet mode ตัด log รายตาราง/progress ออก
เหลือ DIFF, ERROR และสรุป (`migkit -q check mig-a`)

### migkit doctor

ตาราง hop ที่ตั้งไว้ + เช็ค tools + ยิง connection ทุก hop รันก่อนเริ่มงานทุกครั้ง

### migkit advise <hop>

โชว์ playbook ของ service ที่ hop นั้นใช้ — บอกเป็น phase ว่าต้องทำอะไรตามลำดับ:
เตรียม source (parameter อะไรต้องเปิด), เตรียม target (ส่วนที่ migkit ทำให้),
ตั้งค่าอะไรตอนสร้าง task (LOB mode, table prep mode, อะไรห้ามเปิด),
ระหว่างขนดูอะไร, cutover ทำอะไรตามลำดับ, และ rollback ยังไง

```bash
migkit advise mig-a     # playbook ตาม service ที่ตั้งไว้
```

### migkit assess <hop> — precheck ก่อนเริ่ม (แบบ managed service)

ตรวจความพร้อมก่อนเปิดตัวขน ตามแนว precheck/premigration assessment:
version ตรงไหม, CDC prerequisite (wal_level / binlog_format / retention),
ตารางไม่มี PK, unlogged/invalid, encoding+collation, extension ครบไหม,
เทียบ account ระหว่างสองฝั่ง — เขียว/เหลือง/แดง พร้อม exit code

```bash
migkit assess mig-a
```

### หน่วงเช็คซ้ำสำหรับ replication สดๆ (settle)

ใส่ `options: {settle: 30}` ใน hop — เมื่อ data เจอ diff จะรอ 30 วิแล้ว
re-verify เฉพาะ key ที่ต่าง ถ้าหายหมด = in-flight replication ไม่ใช่ diff
จริง (แนวเดียวกับ confirm-out-of-sync ของ enterprise tools)

### migkit schema <hop> — งาน schema ทั้งหมดในคำสั่งเดียว

default = พิมพ์คำสั่งเตรียม schema ปลายทางให้ครบ (pg_dump/pg_restore,
createdb ฯลฯ) **เป็น dry-run เสมอ ไม่รันเอง** — copy ไปรันเองทีละบรรทัด
หัวใจคือ: schema ต้องมาจาก native dump ไม่ใช่จาก migration service
เพราะ migration tools สร้าง schema ไม่ครบ (ไม่มี index รอง, FK, default,
sequence, trigger, view, procedure)

`--migration` = แปลง schema diff ที่เจอเป็นไฟล์ `V<ts>__sync_<db>.sql`
(ทำ target ให้ตรง source) คู่กับ `U<ts>__*.sql` (undo) commit ลง git แล้ว
apply ด้วย psql หรือ migration runner ตัวไหนก็ได้ — ผ่าน atlas

`--convert` = transpile DDL ข้าม engine (hetero hop, sqlglot/pgloader)
review แล้วค่อย `--apply`

```bash
migkit schema mig-a                         # แผนเตรียมปลายทาง (dry-run)
migkit schema mig-a --migration --out migrations
migkit schema my2pg --convert --apply
```

### migkit check --drill — column-level diff (datacompy)

เทียบ row sample ราย column: column ไหนต่าง, match rate, ค่าตัวอย่างที่ไม่ตรง
รายงานอ่านง่ายแบบ PROC COMPARE ของ SAS

```bash
migkit check mig-a --drill --db appdb --table public.orders --limit 1000
```

### migkit report --serve — web dashboard

หน้าเว็บเดียวเห็นทุก hop: summary pills, badge engine/service, tiles สถานะ
ราย check, per-db, ปุ่มเปิด report, feed การเขียนล่าสุดข้าม hop,
auto-refresh 10 วิ (bind localhost อ่านอย่างเดียว)

```bash
migkit report --serve --port 8899
```

### migkit check <hop>

ตัว validate หลัก **อ่านอย่างเดียว 100% ไม่เขียนอะไรทั้งสองฝั่ง ไม่มี lock**
(query ระดับ read แบบเดียวกับ SELECT ปกติ)

เช็ค 4 ชั้นจากเบาไปหนัก:

| ชั้น | เช็คอะไร |
|---|---|
| schema | table, column, PK, FK, index, default, view, procedure, trigger, sequence — ทุก object |
| counts | ตารางครบไหม + จำนวน row เท่ากันเป๊ะทุกตาราง |
| autoinc | ค่า sequence / auto_increment / identity ตรงกันไหม |
| data | checksum ทุก row ทุก column, ถ้าต่างจะ drill ลงถึงระดับ PK ว่าแถวไหนหาย/เกิน/ไม่ตรง |

ตั้งแต่ 0.2.0: ถ้ารัน counts+data ด้วยกัน (default) จำนวน row จะติดมากับ
query checksum เลย — แต่ละตาราง scan รอบเดียว ไม่ใช่สองรอบ (pg/mysql/mongo/
hetero) ส่วนการเช็คว่าตารางครบไหมใช้ catalog อย่างเดียว ไม่ scan

ชั้นที่ 5 (opt-in): `--deep` — FK orphan (หลัง NOT VALID constraint ฝั่ง pg,
scan เต็มฝั่ง mysql เพราะ load มักปิด foreign_key_checks), trigger ที่โดน
disable ค้าง, column drift ราย column (type/null/default/precision +
charset/collation ฝั่ง mysql, เคารพ pattern ใน `<hop>.schema-ignore`),
matview ยังไม่ refresh, grants หาย, และ boundary check — max(pk)/newest
`_id` สองฝั่ง จับทั้ง CDC ค้าง (dst ตามหลัง) และตัวการเขียนใส่ปลายทาง
(dst นำหน้า = double-apply หรือมี writer แปลกปลอม อันหลังนี่แหละต้นเหตุ
คลาสสิกของ dst มี row มากกว่า src)

options:

```bash
migkit check mig-a                          # ครบทุก db ทุกชั้น
migkit check mig-a --only schema,counts     # เลือกชั้น
migkit check mig-a --deep                   # เพิ่มชั้น deep
migkit check mig-a --only deep --db appdb   # deep อย่างเดียว db เดียว
migkit check mig-a --db appdb --table public.orders
migkit check mig-a --workers 6              # ขนาน 6 db
migkit check mig-a --resume                 # ข้ามอันที่เขียวรอบก่อน
migkit -q check mig-a                       # quiet: เหลือ DIFF/ERROR/สรุป
```

การอ่านผล: เขียว OK / เหลือง DIFF / แดง ERROR มี progress + ETA ระหว่างรัน
จบแล้ว exit code 0 = ตรงหมด, 1 = มี diff (เอาไปใช้ใน script/CI ต่อได้)
ทุก DIFF จะพิมพ์บรรทัด `fix:` บอกคำสั่งซ่อมให้เลย

เรื่อง PK กระโดด: การเทียบเป็นราย PK ดังนั้น id ที่โดนลบไป (เช่น 1..188,190,191)
ต้องหายเหมือนกันทั้งสองฝั่งถึงจะผ่าน — ถ้าปลายทางมี 189 โผล่มาจะขึ้นเป็น extra

ความเร็ว: ชั้น data ใช้ checksum แบบบวกได้ (commutative sum of md5) ซึ่ง
postgres ทำ parallel aggregate ได้เต็มเครื่อง — สูตรเดียวกับ reladiff/pg_comparator
ของจริง: ตาราง 488M row ตรวจจบใน 9.4 นาที, ทั้ง database 1.06B row ~16 นาที
ตารางที่ checksum ไม่ตรงเท่านั้นถึงจะ drill ลงหา PK รายแถวด้วยโหมด slice

### migkit watch --verify — เทียบต่อเนื่อง ไม่ต้องรันเป็นรอบเอง

แนวเดียวกับ CDC validation ของ managed service แต่ใช้ได้ทุก engine: วน re-check เอง
ทุก interval, พิมพ์บรรทัดเดียวต่อ db ต่อรอบ, อัพเดท report.html ให้ตลอด
diff ที่หายไปในรอบถัดไป = replication lag, diff ที่ค้าง = ของจริง

```bash
migkit watch mig-a --verify                             # counts+autoinc ทุก 5 นาที
migkit watch mig-a --verify --only counts,autoinc,data  # รวม checksum เต็มทุกรอบ
migkit watch mig-a --verify --count 12                  # 12 รอบแล้วหยุด
```

รันค้างใน tmux ระหว่างการขน data = เฝ้าตลอดคืนได้

### migkit watch <hop>

ใช้ระหว่าง migration service กำลังขน — ดูว่าเชื่อมอยู่ไหม ขนไปถึงไหน เร็วแค่ไหน

```bash
migkit watch mig-a                # วนทุก 30 วิ กด ctrl-c หยุด
migkit watch mig-a --interval 60
migkit watch mig-a --count 1      # ดูครั้งเดียวแล้วออก
```

ตัวอย่าง output กับความหมาย:

```
appdb: src~1,401 dst~1,410 (100%)               <- row โดยประมาณสองฝั่ง
    slot mover_appdb active=true lag=2824 bytes        <- replication slot ฝั่ง source
    conn mover_appdb 10.0.0.9/32 streaming             <- ตัวขนต่ออยู่จริง กำลัง stream
```

- `active=true` + `streaming` = ตัวขนเชื่อมอยู่และทำงาน
- `lag` หลัก byte/KB = ตามทันเกือบ real-time, ถ้าโตเรื่อยๆ เป็น GB = มีปัญหา ตามไปดู console
- ช่วง full load จะเห็น rate (rows/s) + ETA คำนวณให้
- `caught up, check lag before cutover` = จำนวน row ตามทันแล้ว

### migkit sync <hop> — ซ่อมปลายทางให้เท่าต้นทาง (รวม repair เดิม)

สามระดับ จากเบาไปหนัก **default คือ dry-run เสมอ**:

```bash
migkit sync mig-a --db appdb --kind sequences          # ดูแผนก่อน (dry-run)
migkit sync mig-a --db appdb --kind sequences --apply  # ทำจริง + เซฟ undo
migkit sync mig-a --go                                 # เช็ค+ซ่อมรอบเดียว มี checkpoint
```

- ไม่ใส่ flag = โชว์แผนซ่อมจาก diff รอบ check ล่าสุด (ไม่ใส่ `--db` =
  ทุก db ที่มี diff ไม่ต้อง copy-paste ทีละตาราง)
- `--apply` = รันแผนนั้นจริง ค่าเดิมของปลายทางถูกเซฟเป็น undo ก่อนทุกครั้ง
- `--go` = โหมด cutover: checkpoint state ปลายทางก่อน (ค่า sequence ทุกตัว +
  schema dump เก็บ 2 ที่: `reports/<hop>/<db>/state/<ts>/` + tar ที่
  `~/.migkit-state/`) แล้ว snapshot -> เช็ค autoinc (แก้ถ้า diff) -> เช็ค
  data แบบ fast (แก้รายแถวถ้า diff) -> เช็คซ้ำจนเขียว ทุก action ลง
  journal.jsonl

### migkit rollback <hop> --db X [--state TS] [--apply]

ย้อนกลับไป state ไหนก็ได้ที่ sync เซฟไว้:

```bash
migkit rollback mig-a --db appdb                # ดู state ล่าสุด + สิ่งที่จะย้อน
migkit rollback mig-a --db appdb --state 20260723-073539 --apply
```

sequence ย้อนอัตโนมัติ ส่วน row-level จะโชว์ manifest (ไฟล์ undo ที่ fix-data
เซฟ row เดิมไว้ก่อนลบ) ให้รันตาม

| kind | ทำอะไร | rollback |
|---|---|---|
| sequences | setval / reseed ค่า counter ปลายทางให้เท่าต้นทาง (ค่าตรงๆ ไม่ใช่ max+1) | ค่าเดิมถูกเซฟเป็นไฟล์ undo ก่อน apply ทุกครั้ง |
| rows | ลบ+copy เฉพาะ PK ที่ check data รายงานว่า หาย/เกิน/ไม่ตรง | source ไม่ถูกแตะ คือความจริงเสมอ รัน repair ซ้ำได้ |
| all | ทั้งสองอย่าง | - |

ไฟล์ undo อยู่ที่ `reports/<hop>/<db>/undo/<timestamp>-*.sql` เอาไปรันย้อนได้เลย

ส่วน schema diff ไม่มี auto-apply โดยตั้งใจ — DDL ต้อง review เอง:
ดู `schema.diff` แล้ว apply จาก `schema-src.sql` หรือใช้ `migra-fix.sql`
ที่ migra generate ให้ (ถ้าลง migra ไว้)

ข้อควรระวัง: อย่า repair ตอน incremental replication ยังวิ่งแล้วคาดหวังว่าจะนิ่ง —
ต้นทางยังมี write ใหม่เรื่อยๆ ค่าจะ drift ต่อ การซ่อมให้ตรง 100% เป็นงานตอน
cutover (หยุด write แล้ว) เท่านั้น

---

## 4. ไฟล์รายงาน

```
reports/<hop>/summary.json                     ผลรวมรอบล่าสุด (--resume ใช้ไฟล์นี้)
reports/<hop>/report.html                      รายงานอ่านง่าย สร้างใหม่ทุกครั้งที่ check
reports/<hop>/<db>/objects.json                inventory ราย object type
reports/<hop>/<db>/state/<ts>/                 state ของ sync (+ tar ที่ ~/.migkit-state)
reports/<hop>/<db>/undo/                       undo ของ repair (mysql/mongo)
reports/<hop>/<db>/data-<t>.missing/.extra/.changed   คีย์รายแถวที่ต่าง (mysql/mongo)

reports/pgdc/<hop>/<db>/                       รายละเอียดฝั่ง postgres:
  schema-src.sql / schema-dst.sql              dump สองฝั่ง (กรอง object ของ migration tool ตาม pattern ที่ config)
  schema.diff, counts.diff, sequences.diff     ส่วนที่ไม่ตรง (มีเฉพาะตอน diff)
  data-<table>.missing / .extra / .changed     PK รายแถวที่ต่าง เป็น input ของ repair
  undo/                                        row เดิมของ target ก่อนถูกแก้ + manifest
```

ความหมายไฟล์ data: `missing` = source มี target ไม่มี, `extra` = target มีเกิน,
`changed` = มีทั้งคู่แต่ค่าไม่ตรง

---

## 5. Workflow เต็มของการ migrate 1 hop

```
1. migkit doctor                        เช็คเครื่องมือ + connection
2. migkit advise <hop>                  อ่าน playbook ของ service ที่ใช้
3. migkit schema <hop>                  เตรียม schema ปลายทาง (รันคำสั่งเอง)
   - drop/disable FK + trigger ปลายทาง เก็บ DDL ไว้ (เดี๋ยวใส่คืนตอน cutover)
4. migkit check <hop> --only schema     ต้องเขียวก่อนเปิดตัวขน data
5. เปิด migration service ตาม advise              โหมด data-only, full load + incremental
6. migkit watch <hop>                   เฝ้าดูจนขนเสร็จ + incremental ตามทัน
7. ระหว่างนี้ freeze DDL ที่ source     logical replication ไม่ส่ง DDL
--- cutover window ---
8. หยุด write ที่ source, รอ lag = 0 (ดูจาก watch)
9. migkit sync <hop> --db X --kind sequences --apply     ทุก db
10. ใส่ FK + trigger คืนที่ปลายทาง
11. migkit check <hop>                  ต้องเขียวหมดทุกชั้น
12. สลับ app ไปปลายทาง
--- rollback ---
ก่อน cutover: หยุด task เฉยๆ source ไม่ถูกแตะอะไรเลย
หลัง cutover: เปิด replication ขากลับ (dst->src) ซึ่งควรเตรียม task ไว้ก่อนวันจริง
```

---

## 5.1 ตัวอย่าง config engine อื่น

### mysql

```yaml
  shop-mysql:
    engine: mysql
    service: aws-dms
    source: {host: src-mysql.internal, port: 3306, user: root, password: "x"}
    target: {host: dst-mysql.internal, port: 3306, user: root, password: "x"}
    databases: [shop]
```

```bash
migkit check shop-mysql
```

เช็คให้: schema (mysqldump ทั้ง routine/trigger/event, ตัด AUTO_INCREMENT
กับ DEFINER ออกให้แล้ว), ตารางครบ + count ตรง, ค่า auto_increment,
data (reladiff ถ้ามี ไม่มีก็ crc32 checksum ต่อตาราง)
ซ่อม: `repair --kind sequences --apply` ปรับ auto_increment,
row-level ใช้ pt-table-sync (มี --dry-run ในตัว)

### mongodb

```yaml
  shop-mongo:
    engine: mongodb
    source:
      host: src-mongo.internal
      port: 27017
      user: app
      password: "x"
      uri_options: "authSource=admin&replicaSet=rs0"
    target: {host: dst-mongo.internal, port: 27017}
    databases: [shop]
```

```bash
migkit check shop-mongo
```

เช็คให้: collections/views ครบ, index spec ตรง (รวม unique), collection
options (validator), count ต่อ collection, data ด้วย dbHash ฝั่ง server
(เร็วมาก ไม่ดูด data ออกมา) ซ่อม: mongodump/mongorestore --drop
รายคอลเลกชันตามที่ dbHash ชี้

### kafka

```yaml
  events-kafka:
    engine: kafka
    source: {host: src-broker.internal, port: 9092}
    target: {host: dst-broker.internal, port: 9092}
    options: {sample: 500}
```

### generic (snowflake / bigquery / redshift / clickhouse / oracle / ...)

```yaml
  wh-move:
    engine: generic
    source: {options: {url: "snowflake://user:pass@account/db/schema?warehouse=WH"}}
    target: {options: {url: "clickhouse://user:pass@host:9000/db"}}
    options:
      tables: [orders, order_items]
      key: order_id
```

ใช้ reladiff เป็นตัวเทียบ (ลงมากับ bootstrap ใน `.venv-tools`) เช็ค counts
กับ data ราย table ได้ทันทีกับทุก engine ที่ reladiff รู้จัก

```bash
migkit check events-kafka
```

เช็คให้: topic ครบ, จำนวน partition ตรง, end offset ต่อ topic
ขา sync ใช้ MirrorMaker2 ตาม advise ข้อจำกัดตอนนี้: ต่อแบบ plaintext
เท่านั้น ยังไม่รองรับ SASL/TLS และ offset ไม่เท่ากันเป็นเรื่องปกติ
หลัง mirror (ดู lag ของ consumer group ประกอบ)

## 5.2 การประกอบ tools (ของจริงที่ถูกเรียกใช้ ไม่ใช่แค่แนวคิด)

| งาน | tool ที่ใช้ | fallback ถ้าไม่มี |
|---|---|---|
| pg schema | pg_dump diff + object inventory + **liquibase diff** + **atlas** (gen DDL) + **migra** (gen DDL) | pg_dump diff อย่างเดียวก็ครบ |
| mysql schema | mysqldump diff + **atlas** (gen DDL ซ่อม) | mysqldump diff |
| ทางหนีไฟ | **datacompy** (pandas) เทียบใน notebook กรณี engine แปลกๆ | - |
| pg data | builtin parallel sum-of-md5 + slice drilldown | - (เป็นตัวหลักเพราะรองรับ jsonb) |
| mysql schema | mysqldump diff | - |
| mysql data | **reladiff** (hashdiff) -> builtin chunked drilldown | builtin ทั้งหมด |
| mysql row repair | builtin (undo ในตัว) + **pt-table-sync --print** โชว์ SQL ใน dry-run | builtin |
| mongo data | dbHash (server) -> $toHashedIndexKey drilldown | - |
| mongo sync จริง | mongosync + embedded verifier, **migration-verifier** ระหว่าง sync | mongodump/restore |
| kafka | offsets + tail content hash, cutover ดู MM2 heartbeat/checkpoint | - |
| generic | **reladiff** ทุก engine ที่มันรู้จัก | - |

ทุกชั้นเป็นอิสระต่อกัน — ผ่านหลายชั้นพร้อมกัน = ความมั่นใจแบบตรวจไขว้
ตัวไหนหายไปจากเครื่อง feature ไม่ตาย แค่ลดชั้นลง (doctor บอกว่ามีอะไรบ้าง)

## 6. ความสามารถ / ข้อจำกัด per engine

- **postgres**: ครบสุด — schema ระดับ object inventory (จับ invalid index ได้),
  data ผ่าน parallel sum-of-md5 (488M row ~10 นาที) + slice drilldown ราย PK,
  ซ่อมรายแถวพร้อม undo, sequence sync, state snapshot
  ตารางไม่มี PK เทียบได้แค่ checksum รวม (บอกได้ว่าต่าง แต่ drill ไม่ได้)
- **mysql**: recipe เดียวกับ pt-table-checksum (BIT_XOR chunked) แต่ใช้ได้กับ
  2 server อิสระ: checksum เป็น chunk ขนานหลาย connection (mysql ไม่มี parallel
  query ในตัว), drill ราย PK -> missing/extra/changed, ซ่อมรายแถวพร้อม undo,
  auto_increment อ่านแบบสด (เลี่ยง cache ของ information_schema ใน mysql 8)
- **mssql**: sqlcmd + catalog hash ของทุก object definition, data ใช้
  binary_checksum (หยาบ — ยืนยันด้วย reladiff/tablediff ก่อนซ่อม), identity
  ซ่อมด้วย dbcc checkident
- **mongodb**: index/options ตรงระดับ spec, data 2 ชั้น: dbHash ต่อ collection
  (เร็ว) แล้ว drill ราย _id ด้วย $toHashedIndexKey เมื่อไม่ตรง, ซ่อมราย
  document (replace/insert/delete พร้อม undo เป็น extended json)
  งาน sync จริง: mongosync + embedded verifier, ระหว่าง sync ใช้
  mongodb-labs migration-verifier ได้
- **redis**: dbsize + เทียบ key ทุก type (deep: true = ทุก key), sync ใช้ RIOT
- **kafka**: topics/partitions, message count ต่อ topic (end-begin offsets),
  content ท้าย partition แบบ hash (options.sample, default 200 messages),
  cutover ดู heartbeat/checkpoint ของ MirrorMaker2 ประกอบ
- **generic**: อะไรก็ตามที่ reladiff รู้จัก — ใส่ `url` เต็มใน endpoint
  กับ `options.tables` แล้ว check counts/data ได้เลย

## 6.1 Idempotency

- check ทุกชั้น read-only รันซ้ำได้ตลอด ไม่มี lock เกิน SELECT ปกติ
- repair รันซ้ำได้: ลบ+copy ราย key จาก source เดิม ผลลัพธ์ converge
  เป็น state เดียวกันทุกครั้ง, setval/reseed เป็นการตั้งค่าตรงๆ ไม่ใช่บวกเพิ่ม
- undo เป็นไฟล์ append พร้อม timestamp ไม่ทับของเก่า, state snapshot
  เก็บสองที่ (reports + ~/.migkit-state) ย้อนได้ทุกจุดด้วย migkit rollback

## 6.2 Managed services บน cloud

Engine ต่อผ่าน wire protocol มาตรฐาน จึงใช้กับ managed ทุกเจ้าได้
(RDS/Aurora, Cloud SQL/AlloyDB, Azure Database, TencentDB) ต่างกันแค่ quirk
ซึ่งจัดการด้วย fallback ในตัว:

- **DocumentDB (AWS)**: ไม่มี dbHash และ $toHashedIndexKey — engine ตกลง
  ชั้นสามอัตโนมัติ (client-side BSON hash ราย document) config:

```yaml
    source:
      host: docdb-cluster.xxxx.us-east-1.docdb.amazonaws.com
      user: app
      password: "x"
      uri_options: "tls=true&tlsCAFile=global-bundle.pem&retryWrites=false"
```

- **Cosmos DB (mongo api)**: แบบเดียวกับ DocumentDB (fallback ชั้นสาม)
- **TencentDB postgres**: unlogged table ต้อง `set tencentdb_log_unlogged_table=off`
- **RDS postgres**: ใช้ rds_superuser ได้ปกติ รวม session_replication_role
- **ElastiCache/MemoryDB, MSK**: redis/kafka engine ต่อผ่าน endpoint ปกติ
  (MSK ต้อง plaintext listener หรือรอ SASL support)

## 7. Troubleshooting

- **diff เล็กๆ โผล่ทั้งที่เพิ่งซ่อม** — ปกติ ถ้า incremental ยังวิ่ง ต้นทางมี write
  ใหม่ตลอด เช็คซ้ำเฉพาะจุด (`--db X --table Y`) ก่อนสรุป
- **check data ตารางใหญ่ช้า** — ตาม design (hash ทุก row) 488M row ใช้ราวๆ 2-3 ชม.
  รันใน background/tmux แล้วดู progress เอา ปรับ `slice` ใหญ่ขึ้นได้ถ้า server แรง
- **server ฟ้อง no space left (pgsql_tmp)** — เกิดกับ query แบบ sort ทั้งตาราง
  โหมด slice แก้ปัญหานี้แล้ว ถ้าเจอแปลว่าตารางนั้นหลุดไปโหมดธรรมดา (เช่น PK
  เป็น text) — เช็คทีละตารางหรือขยาย temp ฝั่ง server
- **doctor ฟ้อง dst FAIL** — เช็ค VPN/เส้นทาง network ไปปลายทาง แล้วดู firewall ฝั่ง cloud

## 8. กฎเหล็ก

- `check` (รวม `--deep`/`--drill`) อ่านอย่างเดียว รันได้ตลอดเวลา ไม่ต้องขอใคร
- `sync` / `schema` / `move` แตะปลายทาง — dry-run ก่อนเสมอ, `--apply`/`--go`
  เฉพาะตอนตั้งใจ และห้ามรันตอน incremental วิ่งถ้าหวังผลนิ่ง
- source ไม่ถูกเขียนโดย migkit ในทุกกรณี
- schema ปลายทางมาจาก native dump เท่านั้น อย่าให้ migration service สร้าง
- sequence/identity ต้อง repair ทุกครั้งหลัง full load — ไม่มี migration service
  เจ้าไหนขนมาให้
