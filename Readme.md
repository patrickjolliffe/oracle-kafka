# Oracle Snapshot Log CDC with Kafka

CDC using Oracle snapshot logs and Kafka.

## Quick Start

```bash
git clone https://github.com/patrickjolliffe/oracle-kafka.git
cd oracle-kafka
docker compose up -d
```

Wait 30-60s for Oracle to initialize.

## What's Running

- **Oracle Free** on port 1521 with snapshot logs
- **Kafka** on port 9092
- **CDC Reader** polling MLOG$ every 60 seconds
- **Data Generator** inserting test data every 10 seconds
- **Kafka UI** on port 8080

## View Messages

http://localhost:8080

## Insert Test Data

```bash
docker compose exec -T oracle sqlplus -L 'kafka/kafka@//localhost:1521/FREE' @- <<< "
INSERT INTO kafka.data_extraction_history (deh_key, deh_name, deh_status) VALUES (100, 'Test', 'ACTIVE');
COMMIT;
EXIT;
"
```

## Project Structure

```
oracle-kafka/
├── docker-compose.yml
├── oracle/oracle-init.sql
├── cdc/cdc_reader.py
└── data-generator/data_generator.py
```

## How It Works

1. Oracle snapshot logs capture table changes via triggers
2. CDC Reader polls MLOG$ tables every 60 seconds
3. Publishes INSERT/UPDATE/DELETE events to Kafka
4. Initial snapshot on first run (persisted in Oracle)
5. Purges logs older than 24 hours

## Key Features

- ✓ Handles initial snapshot + incremental changes
- ✓ Persistent state (survives restarts)
- ✓ Automatic log purging (24h retention)
- ✓ No LogMiner memory overhead
- ✓ Pure Python CDC reader
- ✓ Test data generator included

## Cleanup

```bash
docker compose down -v
docker volume prune
```

## Docs

- Messages are JSON with `op` (r/c/u/d), `data`, and `ts_ms`
- CDC logs: `docker compose logs -f cdc`
- Oracle init script: `oracle/oracle-init.sql`
- Configuration: Edit intervals in docker-compose.yml or Python files
