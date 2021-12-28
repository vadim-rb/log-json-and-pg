import threading
import json
import tail
from queue import Queue
import psycopg2
from psycopg2 import sql, extras
from psycopg2.extensions import register_adapter
import time
import configparser
import argparse
import logging
import signal

parser = argparse.ArgumentParser(description="Log to Postgres")
parser.add_argument("-c", default='config.ini', type=str, help='Parameter can be used to specify the path where the '
                                                               'configuration file resides')
parser.add_argument("-o", default='main.log', type=str, help='Self log (default main.log)')
parser.add_argument("-d", default='warning', type=str, help='Self level logging (default warning)')

args = parser.parse_args()
config = configparser.ConfigParser()
config.read(args.c)

psycopg2.extensions.register_adapter(dict, extras.Json)

HOST = config['postgresql']['host']
PORT = config['postgresql']['port']
USER = config['postgresql']['user']
PASSWORD = config['postgresql']['password']
DATABASE = config['postgresql']['database']
SCHEMA = config['postgresql']['schema']
LOG_TABLE = config['postgresql']['log_table']

PATTERN_ = config['user']['pattern'].split(',')
PATTERN = list(map(str.strip, PATTERN_))
LOG_FILE = config['user']['log_file']

log_queue = Queue(maxsize=9999)
if args.d == 'debug':
    level = logging.DEBUG
elif args.d == 'info':
    level = logging.INFO
elif args.d == 'warning':
    level = logging.WARNING
elif args.d == 'error':
    level = logging.ERROR
elif args.d == 'critical':
    level = logging.CRITICAL
else:
    level = logging.DEBUG

logging.basicConfig(filename=args.o, level=level, filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def add_to_queue(line):
    log_queue.put(line)


def consumer(q, stop_event):
    pg_conn = None
    pg_cursor = None
    while pg_conn is None:
        try:
            pg_conn = psycopg2.connect(host=HOST,
                                       port=PORT,
                                       user=USER,
                                       password=PASSWORD,
                                       database=DATABASE)
            pg_cursor = pg_conn.cursor()
        except psycopg2.OperationalError:
            logging.error('Error while connecting, try reconnect')
            time.sleep(5)
    logging.debug('Connection success')
    while not stop_event.is_set():
        if not q.empty():
            message = q.get()
            try:
                j = message.split('sql_body:')[1]
                js = json.loads(j)
                logging.debug('Succesfully parsing')
                query = sql.SQL("insert into {} ({}) values ({})").format(
                    sql.Identifier(SCHEMA, LOG_TABLE),
                    sql.SQL(', ').join(map(sql.Identifier, PATTERN)),
                    sql.SQL(', ').join(map(sql.Placeholder, PATTERN)))
                logging.debug(query.as_string(pg_conn))
                pg_cursor.execute(query, js)
                pg_conn.commit()
            except json.decoder.JSONDecodeError as e:
                logging.debug(e)
                continue
            except IndexError as ie:
                logging.debug(ie)
                continue
            except psycopg2.OperationalError:
                logging.exception('')
                q.put(message)
                pg_conn.rollback()
                if pg_cursor is not None:
                    pg_cursor.close()
                if pg_conn is not None:
                    pg_conn.close()
                while pg_conn is None:
                    try:
                        pg_conn = psycopg2.connect(host=HOST,
                                                   port=PORT,
                                                   user=USER,
                                                   password=PASSWORD,
                                                   database=DATABASE)
                        pg_cursor = pg_conn.cursor()
                    except psycopg2.OperationalError:
                        logging.error('Error while connecting except, try reconnect')
                        time.sleep(5)
            except psycopg2.Error:
                logging.exception('')
                if pg_cursor is not None:
                    pg_cursor.close()
                if pg_conn is not None:
                    pg_conn.close()
                return


logging.warning('Start parse')
t = tail.Tail(LOG_FILE)
t.register_callback(add_to_queue)
stop_event = threading.Event()
worker = threading.Thread(target=consumer, args=(log_queue, stop_event,))
worker.daemon = True
worker.start()


def signal_handler(signum, frame):
    global t
    stop_event.set()
    t.stop_event = True
    logging.warning('Stop. Catch signum {}'.format(signum))


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

t.follow()
