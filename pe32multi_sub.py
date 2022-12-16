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

    def get(self):
        if not self._conn:
            self._conn = psycopg2.connect(**self._dsn)
        if self._conn.closed:
            self._conn.connect()
        assert not self._conn.closed, (
            self._dsn['database'], self._conn.closed)
        return self._conn

    def cursor(self):
        return self.get().cursor()

    def get_row(self, query, *args):
        with self.cursor() as cursor:
            cursor.execute(query, args)
            ret = cursor.fetchall()
        assert len(ret) == 1, (query, args, ret)
        return ret[0]

    def put_row(self, query, *args):
        with self.get() as conn:  # needed for transaction..
            with conn.cursor() as cursor:
                cursor.execute(query, args)


class Pe32Writer:
    TABLE_MAP = {
        # FIXME: add limits here..? temperature never <50 or >120?
        'temperature': ('temperature', float),
        'humidity': ('humidity', float),
        'heatindex': ('heatindex', float),
        'dewpoint': ('dewpoint', float),
        'comfortidx': ('comfortidx', float),
        'comfort': ('comfort', str),
    }

    def __init__(self, dbconn):
        self._dbconn = dbconn

    def get_device(self, device_id):
        return self._dbconn.get_row(
            'SELECT id, dev_type, label_id FROM device WHERE identifier = %s',
            device_id)

    def on_measure(self, device_id, measure, str_value):
        device_id, dev_type, label_id = self.get_device(device_id)
        table, caster = self.TABLE_MAP.get(measure, (None, None))
        if not table or not label_id:
            log.info(
                f'ignoring {device_id} {dev_type} {label_id} {measure} '
                f'{str_value}')
            return

        timestamp = (time.time() // 60) * 60
        value = caster(str_value)
        # FIXME: must do with on the conn as well.. otherwise not transaction?
        if caster == str:
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


configure_logging()
dbconn = DatabaseConnection.create_default()
writer = Pe32Writer(dbconn)
relay = Pe32Relay(writer)
client = mqtt.Client()
client.on_connect = relay.on_connect
client.on_message = relay.on_message

client.connect('localhost', 1883, 60)
client.loop_forever()
raise NotImplementedError('should not get here')
