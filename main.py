import zlib
import logging
from datetime import datetime, timedelta

import simplejson
from gevent.pool import Pool
import zmq.green as zmq
import MySQLdb

import settings


def init_Subscriber(context, address):
    subscriber = context.socket(zmq.SUB)
    subscriber.RCVTIMEO = settings.MAX_WAIT_TIME
    subscriber.connect(address)
    subscriber.setsockopt(zmq.SUBSCRIBE, "")
    return subscriber;


def start_ZMQ():
    i = 0
    address_index = 0
    db_connections = []

    # Init ZMQ listener
    context = zmq.Context()
    subscriber = init_Subscriber(context, settings.ZMQ_ADDRESS[address_index])
    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    logging.info("ZMQ started on {}".format(settings.ZMQ_ADDRESS[address_index]))

    # Init threadpool
    greenlet_pool = Pool(size=settings.MAX_NUM_POOL_WORKERS)
    logging.info("Consumer daemon started, waiting for jobs...")

    # Init db connections
    for j in range(settings.DB_CONNECTIONS):
        db_connections.append(MySQLdb.connect(host=settings.DB_HOST, user=settings.DB_USER, passwd=settings.DB_PASSWD,
                                              db=settings.DB_NAME))

    while True:
        socks = dict(poller.poll(60000))
        if socks:
            # Create thread
            if socks.get(subscriber) == zmq.POLLIN:
                greenlet_pool.spawn(worker_ZMQ, subscriber.recv(), db_connections[i])
                i = i + 1 if i < settings.DB_CONNECTIONS - 1 else 0
        else:
            # Reconnect on next address
            logging.info("TIMEOUT ON {}".format(settings.ZMQ_ADDRESS[address_index]))
            address_index = (address_index < len(settings.ZMQ_ADDRESS) - 1) if address_index + 1 else 0
            subscriber = init_Subscriber(context, settings.ZMQ_ADDRESS[address_index])
            poller = zmq.Poller()
            poller.register(subscriber, zmq.POLLIN)
            logging.info("ZMQ started on {}".format(settings.ZMQ_ADDRESS[address_index]))


def worker_ZMQ(job_json, connection):
    market_json = zlib.decompress(job_json)
    market_data = simplejson.loads(market_json)

    # Iterate rowsets
    for row in market_data['rowsets']:
        # Do if item in region
        if row['regionID'] in settings.REGION:
            # Save/Update market orders
            if market_data['resultType'] == 'orders':
                save_update_orders(row, connection.cursor())

            # Save/Update market history
            if market_data['resultType'] == 'history':
                save_update_history(row, connection.cursor())

    # Commit
    connection.commit()


def save_update_history(history, cursor):
    startTime = datetime.now()
    state = ''
    region = settings.REGION[history['regionID']]
    generated_at = datetime.strptime(history['generatedAt'], '%Y-%m-%dT%H:%M:%S+00:00')
    history_id = 0
    try:
        cursor.execute("SELECT id, generatedAt FROM markethistory WHERE region='{}' AND item_typeID={}".format(
            region, history['typeID']))
        history_db = cursor.fetchone()
        if history_db is None:
            cursor.execute(
                "INSERT INTO markethistory (generatedAt, region, item_typeID, updatedAt) VALUES ('{}', '{}', {}, CURRENT_TIMESTAMP)".format(
                    generated_at, region, history['typeID']))
            state = 'CREATED'
            history_id = cursor.lastrowid
        elif history_db[1] < generated_at:
            cursor.execute("UPDATE markethistory SET generatedAt='{}', updatedAt=CURRENT_TIMESTAMP, updateCount=updateCount+1 WHERE id={}".format(
                generated_at.strftime('%Y-%m-%d %H:%M:%S'), history_db[0]))
            state = 'UPDATED'
            history_id = history_db[0]
        if history_id:
            query = "INSERT INTO history (date, orders, quantity, low, high, average, marketHistory_id) " \
                    "VALUES ('{}', {}, {}, {}, {}, {}, {}) " \
                    "ON DUPLICATE KEY UPDATE average=VALUES(average), high=VALUES(high), " \
                    "low=VALUES(low), orders=VALUES(orders), quantity=VALUES(quantity)"
            for row in history['rows']:
                history_date = datetime.strptime(row[0], '%Y-%m-%dT%H:%M:%S+00:00')
                if (datetime.now() - history_date) < timedelta(days=31):
                    cursor.execute(
                        query.format(history_date.strftime('%Y-%m-%d %H:%M:%S'), row[1], row[2], row[3], row[4], row[5],
                                     history_id))
    except Exception, e:
        logging.exception(e)
    if state:
        execution_time = (datetime.now() - startTime).microseconds / 1000
        logging.info('HISTORY\t{}\t\t{}\t{}\t{}\t{}ms'.format(state, str(
            history['typeID']).ljust(10), region.ljust(15), generated_at, execution_time))


def save_update_orders(data, cursor):
    startTime = datetime.now()
    state = ''
    region = settings.REGION[data['regionID']]
    generated_at = datetime.strptime(data['generatedAt'], '%Y-%m-%dT%H:%M:%S+00:00')
    data_id = 0
    try:
        cursor.execute("SELECT id, generatedAt FROM marketdata WHERE region='{}' AND item_typeID={}".format(
            region, data['typeID']))
        data_db = cursor.fetchone()
        if data_db is None:
            cursor.execute(
                "INSERT INTO marketdata (generatedAt, region, item_typeID, updatedAt) VALUES ('{}', '{}', {}, CURRENT_TIMESTAMP)".format(
                    generated_at, region, data['typeID']))
            state = 'CREATED'
            data_id = cursor.lastrowid
        elif data_db[1] < generated_at:
            cursor.execute("UPDATE marketdata SET generatedAt='{}', updatedAt=CURRENT_TIMESTAMP, updateCount=updateCount+1 WHERE id={}".format(
                generated_at.strftime('%Y-%m-%d %H:%M:%S'), data_db[0]))
            state = 'UPDATED'
            data_id = data_db[0]
        if data_id:
            query = "INSERT INTO marketorder (issueDate, price, volRemaining, orderID, volEntered, minVolume, bid, " \
                    "duration, station, solarSystem, marketData_id) " \
                    "VALUES ('{}', {}, {}, {}, {}, {}, {}, {}, '{}', '{}', {}) "
            cursor.execute("DELETE FROM marketorder WHERE marketData_id={}".format(data_id))
            for row in data['rows']:
                if row[9] in settings.STATION:
                    order_date = datetime.strptime(row[7], '%Y-%m-%dT%H:%M:%S+00:00')
                    cursor.execute(
                        query.format(order_date.strftime('%Y-%m-%d %H:%M:%S'), row[0], row[1], row[3], row[4], row[5],
                                     row[6], row[8], settings.STATION[row[9]], settings.SOLARSYSTEM[row[10]], data_id))
    except Exception, e:
        logging.exception(e)
    if state:
        execution_time = (datetime.now() - startTime).microseconds / 1000
        logging.info('ORDERS \t{}\t\t{}\t{}\t{}\t{}ms'.format(state, str(
            data['typeID']).ljust(10), region.ljust(15), generated_at, execution_time))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    start_ZMQ()
