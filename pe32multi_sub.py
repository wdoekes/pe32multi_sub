#!/usr/bin/env python3
from base64 import b64decode
import logging
import os
import paho.mqtt.client as mqtt
import psycopg2
import sys
import time
import traceback

from settings import (
    DATABASE_NAME,
    DATABASE_HOSTNAME,
    DATABASE_USERNAME,
    DATABASE_PASSWORD_BASE64)


log = logging.getLogger(__file__.rsplit('.', 1)[0])


def configure_logging():
    called_from_cli = (
        # Reading just JOURNAL_STREAM or INVOCATION_ID will not tell us
        # whether a user is looking at this, or whether output is passed to
        # systemd directly.
        any(os.isatty(i.fileno())
            for i in (sys.stdin, sys.stdout, sys.stderr)) or
        not os.environ.get('JOURNAL_STREAM'))
    sys.stdout.reconfigure(line_buffering=True)  # PYTHONUNBUFFERED, but better
    logging.basicConfig(
        level=(
            logging.DEBUG if os.environ.get('DEBUG', '')
            else logging.INFO),
        format=(
            '%(asctime)s %(message)s' if called_from_cli
            else '%(message)s'),
        stream=sys.stdout,
        datefmt='%Y-%m-%d %H:%M:%S')


class DatabaseConnection:
    @classmethod
    def create_default(cls):
        return cls(cls.get_dsn())

    @classmethod
    def get_dsn(cls):
        return {
            'host': DATABASE_HOSTNAME,
            'user': DATABASE_USERNAME,
            'database': DATABASE_NAME,
            'password': b64decode(DATABASE_PASSWORD_BASE64).decode('ascii'),
        }

    def __init__(self, dsn):
        self._dsn = dsn
        self._conn = None

    @property
    def introspection(self):
        if not hasattr(self, '_introspection'):
            self._introspection = DatabaseIntrospection(self)
        return self._introspection

    def get(self):
        if not self._conn:
            self._conn = psycopg2.connect(**self._dsn)
        if self._conn.closed:
            self._conn = psycopg2.connect(**self._dsn)
        assert not self._conn.closed, (
            self._dsn['database'], self._conn.closed)
        return self._conn

    def get_row(self, query, *args):
        with self.get() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, args)
                ret = cursor.fetchall()
        assert len(ret) == 1, (query, args, ret)
        return ret[0]

    def put_row(self, query, *args):
        with self.get() as conn:  # needed for transaction..
            with conn.cursor() as cursor:
                cursor.execute(query, args)


class DatabaseIntrospection:
    def __init__(self, dbconn):
        self._dbconn = dbconn
        self._catalog = dbconn.get_dsn()['database']
        self._schema = 'public'

    @property
    def tables(self):
        if not hasattr(self, '_tables'):
            with dbconn.get() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f'''\
                        SELECT table_name FROM information_schema.tables
                        WHERE table_catalog = '{self._catalog}'
                            AND table_schema = '{self._schema}'
                            AND table_type = 'BASE TABLE'
                    ''')
                    self._tables = list(sorted(
                        row[0] for row in cursor.fetchall()))
        return self._tables

    def columns_for(self, table):
        if not hasattr(self, '_columns_for'):
            assert not any("'" in tbl for tbl in self.tables), self.tables
            tables_str = ','.join(f"'{tbl}'" for tbl in self.tables)
            with dbconn.get() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f'''\
                        SELECT table_name, column_name, is_nullable, data_type
                        FROM information_schema.columns
                        WHERE table_catalog = '{self._catalog}'
                            AND table_schema = '{self._schema}'
                            AND table_name IN ({tables_str})
                    ''')
                    res = cursor.fetchall()
            columns_for = {}
            for row in res:
                table_name, column_name, props = row[0], row[1], row[2:]
                if table_name not in columns_for:
                    columns_for[table_name] = {}
                columns_for[table_name][column_name] = {
                    'is_nullable': props[0] != 'NO',
                    'data_type': props[1],
                }
            self._columns_for = columns_for

        return self._columns_for[table]


class Pe32DataTables:
    """
    Get data table mapping/marshalling.

    There are two "system" tables: device, label

    The rest are expected to take floats or strings. The floating point ones
    also get min/max columns.
    """
    _DB_TS = 'timestamp with time zone'
    _DB_FK = 'integer'
    _DB_NUM = 'real'
    _DB_STR = 'character varying'
    _NUMERIC_TABLE_LAYOUT = {
        'time': {'is_nullable': False, 'data_type': _DB_TS},
        'label_id': {'is_nullable': False, 'data_type': _DB_FK},
        'avg': {'is_nullable': False, 'data_type': _DB_NUM},  # average
        'med': {'is_nullable': False, 'data_type': _DB_NUM},  # median
        'low': {'is_nullable': True, 'data_type': _DB_NUM},
        'high': {'is_nullable': True, 'data_type': _DB_NUM},
    }
    _TEXT_TABLE_LAYOUT = {
        'time': {'is_nullable': False, 'data_type': _DB_TS},
        'label_id': {'is_nullable': False, 'data_type': _DB_FK},
        'value': {'is_nullable': False, 'data_type': _DB_STR},
    }

    @classmethod
    def from_database(cls, dbconn):
        # FIXME: add limits somewhere.? temperature never <50 or >120?
        # Needs to be added to marshaller below.
        ret = cls()

        for table in dbconn.introspection.tables:
            columns = dbconn.introspection.columns_for(table)
            if table in ('device', 'label'):
                pass
            elif columns == cls._NUMERIC_TABLE_LAYOUT:
                ret.add_numeric(table)
            elif columns == cls._TEXT_TABLE_LAYOUT:
                ret.add_text(table)
            else:
                log.info(f'Skipping table {table} with columns {columns}')

        return ret

    def __init__(self):
        self._tables = {}

    def add_numeric(self, table):
        self._tables[table] = (table, float)  # table_name, marshaller

    def add_text(self, table):
        self._tables[table] = (table, str)  # table_name, marshaller

    def get(self, table):
        return self._tables.get(table, (None, None))


class Pe32Writer:
    def __init__(self, dbconn, tables):
        self._dbconn = dbconn
        self._tables = tables

    def get_device(self, device_id):
        return self._dbconn.get_row(
            'SELECT id, dev_type, label_id FROM device WHERE identifier = %s',
            device_id)

    def on_measure(self, device_id, measure, str_value):
        device_id, dev_type, label_id = self.get_device(device_id)
        table, marshaller = self._tables.get(measure)
        if not table or not label_id:
            log.info(
                f'ignoring {device_id} {dev_type} {label_id} {measure} '
                f'{str_value}')
            return

        timestamp = (time.time() // 60) * 60
        value = marshaller(str_value)
        if marshaller == str:
            self._dbconn.put_row(
                f'INSERT INTO {table} (time, label_id, value) VALUES '
                f'(to_timestamp(%s), %s, %s)',
                timestamp, label_id, value)
        else:
            self._dbconn.put_row(
                f'INSERT INTO {table} (time, label_id, low, avg, high) VALUES '
                f'(to_timestamp(%s), %s, %s, %s, %s)',
                timestamp, label_id, value, value, value)


class Pe32Relay:
    def __init__(self, pe32writer, pe32topic='pe32/#'):
        self._topic = pe32topic
        self._writer = pe32writer

    def on_connect(self, client, userdata, flags, rc):
        log.info("Connected with result code {}".format(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(self._topic)  # can do multiple of these

    def on_message(self, client, userdata, msg):
        assert userdata is None, userdata
        try:
            # msg.topic:
            #   pe32/ossohq/buildtime/EUI48:C0:49:EF:D0:1F:38
            # msg.payload:
            #   '2022-12-15T14:43:36+01:00'
            # print(dir(msg))
            # print(msg.info)
            # print(msg.timestamp)
            self.on_payload(msg.topic, msg.payload.decode('ascii'))
        except Exception as e:
            log.error(f'{type(e)}: {msg.payload} -> {e}')
            traceback.print_exc()

    def on_payload(self, topic, payload):
        topic = topic.split('/')
        if len(topic) != 4:
            raise ValueError(f'cannot handle topic {topic}')

        # pe32/ossohq/buildversion/EUI48:C0:49:EF:D0:1F:38
        proto, namespace, measure, device_id = topic
        assert proto == 'pe32', topic
        assert namespace == 'ossohq', topic

        self.on_measure(device_id, measure, payload)

    def on_measure(self, device_id, measure, payload):
        self._writer.on_measure(device_id, measure, payload)


def loop_forever():
    configure_logging()
    dbconn = DatabaseConnection.create_default()
    datatables = Pe32DataTables.from_database(dbconn)
    writer = Pe32Writer(dbconn, datatables)
    relay = Pe32Relay(writer)
    client = mqtt.Client()
    client.on_connect = relay.on_connect
    client.on_message = relay.on_message

    client.connect('localhost', 1883, 60)
    client.loop_forever()
    raise NotImplementedError('should not get here')


if __name__ == '__main__':
    if sys.argv[1:2] == ['relay']:
        loop_forever()
    elif sys.argv[1:2] == ['devices']:
        configure_logging()
        dbconn = DatabaseConnection.create_default()
        with dbconn.get() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''\
                    SELECT d.id, l.id, d.identifier, l.name, d.dev_type,
                        d.version_string
                    FROM label l
                    LEFT JOIN device d on d.label_id = l.id
                    ORDER BY l.name, d.identifier
                ''')
                for row in cursor.fetchall():
                    device_id, label_id, rest = row[0], row[1], row[2:]
                    print(f'- {rest} (#{device_id}->{label_id})')
    elif sys.argv[1:2] == ['set_label'] and len(sys.argv) == 4:
        configure_logging()
        dbconn = DatabaseConnection.create_default()
        device_id = int(sys.argv[2])
        label_id = int(sys.argv[3]) if sys.argv[3] else None
        with dbconn.get() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''\
                    UPDATE device SET label_id = %s WHERE id = %s
                ''', (label_id, device_id))
    else:
        configure_logging()
        dbconn = DatabaseConnection.create_default()
        # for table in dbconn.introspection.tables:
        #     print(dbconn.introspection.columns_for(table))
        datatables = Pe32DataTables.from_database(dbconn)
        print(datatables._tables)
        raise NotImplementedError(f'unexpected command: {sys.argv}')
