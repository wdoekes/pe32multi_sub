-- apt-get install postgresql
-- echo 'deb [signed-by=/etc/apt/keyrings/timescale.asc] https://packagecloud.io/timescale/timescaledb/ubuntu jammy main' >/etc/apt/sources.list.d/timescaledb.list
-- wget -qO/etc/apt/keyrings/timescale.asc https://packagecloud.io/timescale/timescaledb/gpgkey
-- apt-get update
-- apt-get install apt-get install timescaledb-2-postgresql-14
-- echo "shared_preload_libraries = 'timescaledb'" >>/etc/postgresql/14/main/postgresql.conf
--
-- (run in the correct database)> create extension timescaledb;  -- view with \dx
-- (run in the correct database)> set role plot_writer;  -- when logging in as postgres

CREATE TABLE label (
  id SERIAL NOT NULL,
  name VARCHAR(31) NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE device (
  id SERIAL NOT NULL,
  identifier VARCHAR(31) NOT NULL UNIQUE,
  dev_type VARCHAR(31) NOT NULL, -- enum?
  label_id INT NULL, -- references label, unless NULL/new
  version_string VARCHAR(127) DEFAULT NULL,
  CONSTRAINT fk_device FOREIGN KEY (label_id) REFERENCES label(id),
  PRIMARY KEY(id),
  UNIQUE (identifier)
);

--
CREATE TABLE temperature (
  time        TIMESTAMPTZ  NOT NULL,
  label_id    INT          NOT NULL,
  avg         REAL         NOT NULL,
  low         REAL         NULL,
  high        REAL         NULL,
  CONSTRAINT fk_temperature FOREIGN KEY (label_id) REFERENCES label(id),
  UNIQUE (time, label_id)
);
SELECT create_hypertable('temperature', 'time');

--
CREATE TABLE humidity (
  time        TIMESTAMPTZ  NOT NULL,
  label_id    INT          NOT NULL,
  avg         REAL         NOT NULL,
  low         REAL         NULL,
  high        REAL         NULL,
  CONSTRAINT fk_humidity FOREIGN KEY (label_id) REFERENCES label(id),
  UNIQUE (time, label_id)
);
SELECT create_hypertable('humidity', 'time');

--
CREATE TABLE comfortidx (
  time        TIMESTAMPTZ  NOT NULL,
  label_id    INT          NOT NULL,
  avg         REAL         NOT NULL,
  low         REAL         NULL,
  high        REAL         NULL,
  CONSTRAINT fk_comfortidx FOREIGN KEY (label_id) REFERENCES label(id),
  UNIQUE (time, label_id)
);
SELECT create_hypertable('comfortidx', 'time');

--
CREATE TABLE dewpoint (
  time        TIMESTAMPTZ  NOT NULL,
  label_id    INT          NOT NULL,
  avg         REAL         NOT NULL,
  low         REAL         NULL,
  high        REAL         NULL,
  CONSTRAINT fk_dewpoint FOREIGN KEY (label_id) REFERENCES label(id),
  UNIQUE (time, label_id)
);
SELECT create_hypertable('dewpoint', 'time');

--
CREATE TABLE heatindex (
  time        TIMESTAMPTZ  NOT NULL,
  label_id    INT          NOT NULL,
  avg         REAL         NOT NULL,
  low         REAL         NULL,
  high        REAL         NULL,
  CONSTRAINT fk_heatindex FOREIGN KEY (label_id) REFERENCES label(id),
  UNIQUE (time, label_id)
);
SELECT create_hypertable('heatindex', 'time');

--
CREATE TABLE comfort (
  time        TIMESTAMPTZ  NOT NULL,
  label_id    INT          NOT NULL,
  value       VARCHAR(31)  NOT NULL,
  CONSTRAINT fk_comfort FOREIGN KEY (label_id) REFERENCES label(id),
  UNIQUE (time, label_id)
);
SELECT create_hypertable('comfort', 'time');

--
CREATE TABLE mq135rzero (
  time        TIMESTAMPTZ  NOT NULL,
  label_id    INT          NOT NULL,
  avg         REAL         NOT NULL,
  low         REAL         NULL,
  high        REAL         NULL,
  CONSTRAINT fk_mq135rzero FOREIGN KEY (label_id) REFERENCES label(id),
  UNIQUE (time, label_id)
);
SELECT create_hypertable('mq135rzero', 'time');

--
CREATE TABLE mq135rawppm (
  time        TIMESTAMPTZ  NOT NULL,
  label_id    INT          NOT NULL,
  avg         REAL         NOT NULL,
  low         REAL         NULL,
  high        REAL         NULL,
  CONSTRAINT fk_mq135rawppm FOREIGN KEY (label_id) REFERENCES label(id),
  UNIQUE (time, label_id)
);
SELECT create_hypertable('mq135rawppm', 'time');

--
CREATE TABLE mq135corrppm (
  time        TIMESTAMPTZ  NOT NULL,
  label_id    INT          NOT NULL,
  avg         REAL         NOT NULL,
  low         REAL         NULL,
  high        REAL         NULL,
  CONSTRAINT fk_mq135corrppm FOREIGN KEY (label_id) REFERENCES label(id),
  UNIQUE (time, label_id)
);
SELECT create_hypertable('mq135corrppm', 'time');

--
INSERT INTO label (name) values ('ergens/fixme');
INSERT INTO device (identifier, dev_type, label_id) values ('EUI48:11:22:33:44:55:66', 'dht22-v0.1', 1);

--
-- https://www.keyvanfatehi.com/2021/07/14/how-to-create-read-only-user-in-postgresql/
-- GRANT USAGE ON SCHEMA public TO plot_reader;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO plot_reader;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO plot_reader;
-- grant plot_reader to plot_grafana;
-- (again...) GRANT SELECT ON ALL TABLES IN SCHEMA public TO plot_reader;
