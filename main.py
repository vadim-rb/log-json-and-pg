import threading
import tail
from queue import Queue
import configparser
import argparse
import logging
import signal
from consumer import Consumer

parser = argparse.ArgumentParser(description="Log to Postgres")
parser.add_argument("-c", default='config.ini', type=str, help='Parameter can be used to specify the path where the '
                                                               'configuration file resides')
parser.add_argument("-o", default='main.log', type=str, help='Self log (default main.log)')
parser.add_argument("-d", default='debug', type=str, help='Self level logging (default warning)')
args = parser.parse_args()

config = configparser.ConfigParser()
config.read(args.c)
LOG_FILE = config['user']['log_file']

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

log_queue = Queue(maxsize=9999)


def add_to_queue(line):
    log_queue.put(line)


logging.warning('Start parse')
t = tail.Tail(LOG_FILE)
t.register_callback(add_to_queue)
stop_event = threading.Event()
thread1 = Consumer(stop_event, log_queue, config)
thread1.start()


def signal_handler(signum, frame):
    global t
    stop_event.set()
    t.stop_event = True
    logging.warning('Stop. Catch signum {}'.format(signum))


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

t.follow()
