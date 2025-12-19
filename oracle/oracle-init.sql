-- Enable supplemental logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Create kafka user
CREATE USER kafka IDENTIFIED BY kafka;
GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE TRIGGER TO kafka;

-- Grant unlimited tablespace quota
ALTER USER kafka QUOTA UNLIMITED ON USERS;

-- Create test tables
CREATE TABLE kafka.data_extraction_history (
  deh_key NUMBER PRIMARY KEY,
  deh_name VARCHAR2(100),
  deh_status VARCHAR2(50),
  created_at TIMESTAMP DEFAULT SYSDATE
);

CREATE TABLE kafka.debug_data (
  dbg_seq NUMBER PRIMARY KEY,
  dbg_message VARCHAR2(500),
  dbg_level VARCHAR2(10),
  created_at TIMESTAMP DEFAULT SYSDATE
);

-- Create snapshot logs
CREATE SNAPSHOT LOG ON kafka.data_extraction_history
  WITH ROWID
  INCLUDING NEW VALUES;

CREATE SNAPSHOT LOG ON kafka.debug_data
  WITH ROWID
  INCLUDING NEW VALUES;

-- Grant permissions to snapshot logs
GRANT SELECT ON kafka.MLOG$_DATA_EXTRACTION_HISTORY TO kafka;
GRANT SELECT ON kafka.MLOG$_DEBUG_DATA TO kafka;

-- Create CDC state table
CREATE TABLE kafka.cdc_state (
  state_key VARCHAR2(50) PRIMARY KEY,
  state_value VARCHAR2(100),
  updated_at TIMESTAMP DEFAULT SYSDATE
);

INSERT INTO kafka.cdc_state VALUES ('snapshot_done', 'false', SYSDATE);
COMMIT;

-- Insert test data
INSERT INTO kafka.data_extraction_history (deh_key, deh_name, deh_status) VALUES (1, 'Initial Extract 1', 'COMPLETED');
INSERT INTO kafka.data_extraction_history (deh_key, deh_name, deh_status) VALUES (2, 'Initial Extract 2', 'IN_PROGRESS');
INSERT INTO kafka.debug_data (dbg_seq, dbg_message, dbg_level) VALUES (1, 'System initialized', 'INFO');
COMMIT;

-- Verify
SELECT * FROM user_snapshot_logs;