import json
import logging
import threading
import time

import psycopg2
from psycopg2 import sql, extras
from psycopg2.extensions import register_adapter

psycopg2.extensions.register_adapter(dict, extras.Json)


class Consumer(threading.Thread):
    def __init__(self, stop_event, q, config):
        threading.Thread.__init__(self, daemon=True)
        self.stop_event = stop_event
        self.q = q
        self.HOST = config['postgresql']['host']
        self.PORT = config['postgresql']['port']
        self.USER = config['postgresql']['user']
        self.PASSWORD = config['postgresql']['password']
        self.DATABASE = config['postgresql']['database']
        self.SCHEMA = config['postgresql']['schema']
        self.LOG_TABLE = config['postgresql']['log_table']

        PATTERN_ = config['user']['pattern'].split(',')
        self.PATTERN = list(map(str.strip, PATTERN_))
        self.SPLITTER = config['user']['splitter']

    def run(self):
        pg_conn = None
        pg_cursor = None
        while pg_conn is None:
            try:
                pg_conn = psycopg2.connect(host=self.HOST,
                                           port=self.PORT,
                                           user=self.USER,
                                           password=self.PASSWORD,
                                           database=self.DATABASE)
                pg_cursor = pg_conn.cursor()
            except psycopg2.OperationalError:
                logging.error('Error while connecting, try reconnect')
                time.sleep(5)
        logging.debug('Connection success')
        while not self.stop_event.is_set():
            if not self.q.empty():
                message = self.q.get()
                try:
                    j = message.split(self.SPLITTER)[1]
                    js = json.loads(j)
                    logging.debug('Succesfully parsing')
                    query = sql.SQL("insert into {} ({}) values ({})").format(
                        sql.Identifier(self.SCHEMA, self.LOG_TABLE),
                        sql.SQL(', ').join(map(sql.Identifier, self.PATTERN)),
                        sql.SQL(', ').join(map(sql.Placeholder, self.PATTERN)))
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
                    self.q.put(message)
                    pg_conn.rollback()
                    if pg_cursor is not None:
                        pg_cursor.close()
                    if pg_conn is not None:
                        pg_conn.close()
                    while pg_conn is None:
                        try:
                            pg_conn = psycopg2.connect(host=self.HOST,
                                                       port=self.PORT,
                                                       user=self.USER,
                                                       password=self.PASSWORD,
                                                       database=self.DATABASE)
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
                except KeyError as ke:
                    logging.debug(ke)
                    continue
