# /bin/python
# coding=utf-8

import sys
import re
from kafka import KafkaConsumer,TopicPartition
import os
import MySQLdb
import ConfigParser
import time
import logging
import argparse
import ujson
import signal

reload(sys)
sys.setdefaultencoding('utf8')
from warnings import filterwarnings
filterwarnings('error', category=MySQLdb.Warning)



def kafka_consumer(group, server):
    consumer = KafkaConsumer(group_id=group, bootstrap_servers=server, auto_offset_reset='earliest',enable_auto_commit=True,
                             auto_commit_interval_ms = 4000)
    return consumer


def config(configfile):
    config = ConfigParser.ConfigParser()
    config.read(configfile)
    return config


def compare_items(items):
    """caution: if v is NULL, may need to process"""
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def dbsync_run(consumer,offset_log,dbuser,dbpassword,host,dbport):
    # define null,false,true string,if not eval(msg.value) will be error

    null = None
    false = False
    true = True


    db_connect = MySQLdb.connect(user=dbuser, passwd=dbpassword, host=host, port=dbport,charset="UTF8")

    cur = db_connect.cursor()
    for msg in consumer:

        # print msg

        try:
            msg_date = ujson.loads(msg.value)
        except BaseException as e:
            errordate = """error:%s""" % (e)
            logger.error(errordate)
            logger.error(msg.value)
            continue

        # get update date the list member number
        number = -1

        if msg_date.get('type') == 'INSERT':
            clume = msg_date.get('mysqlType').keys()

            # get insert all the vaules to list v_list
            v_list = []
            for l in msg_date.get('data'):
                d = tuple(l.values())
                v_list.append(d)

            sqlcmd = """INSERT  INTO %s.%s(%s) VALUES(%s) """ % (
            msg_date.get('database'), msg_date.get('table'), ','.join(clume), ','.join(['%s' for k in clume]))

            try:


                cur.executemany(sqlcmd, v_list)

                with open(offset_log, 'w') as f:
                    offsetinfo = """servers:%s\ntopic:%s\noffset:%s\npartition:%s\ntimestamp:%s""" % (
                        kafka_server, kafka_topic, msg.offset, msg.partition, msg.timestamp)
                    f.write(offsetinfo)
                db_connect.commit()

            except MySQLdb.Error as e:
                logger.error(e)
                logger.error(sqlcmd)
                logger.error(msg.offset)
                logger.error(msg.timestamp)
                time.sleep(4)
                sys.exit(1)

            except MySQLdb.Warning as e:
                logger.error('%s: %s' % ('Warning', e))
                logger.error(sqlcmd)


        elif msg_date.get('type') == 'UPDATE':

            update_before = msg_date.get('old')
            update_after = msg_date.get('data')

            for l in update_after:

                number += 1
                v_before = {}

                # update_before only have chanage data,so get data from update_after
                for key in l.keys():
                    if update_before[number].get(key):
                        v_before[key] = update_before[number].get(key)
                    else:
                        v_before[key] = l.get(key)
                sqlcmd = """UPDATE %s.%s SET %s WHERE %s LIMIT 1""" % (
                    msg_date.get('database'), msg_date.get('table'), ', '.join(['`%s`=%%s' % k for k in l.keys()]),
                    ' AND '.join(map(compare_items, v_before.items())))
                values = l.values() + v_before.values()

                try:
                    # sqlcmd = db_connect.mogrify(sqlcmd, values)
                    cur.execute(sqlcmd,values)

                    with open(offset_log, 'w') as f:
                        offsetinfo = """servers:%s\ntopic:%s\noffset:%s\npartition:%s\ntimestamp:%s""" % (
                            kafka_server, kafka_topic, msg.offset, msg.partition, msg.timestamp)
                        f.write(offsetinfo)
                    db_connect.commit()



                    # if data Changed not equal to 1,target database data may not be consistent
                    if cur.rowcount != 1:
                        logger.error('The target instance does not have this rows ,the data may not be consistent')
                        logger.error(sqlcmd)
                        logger.error(values)
                        logger.error(msg.offset)
                        logger.error(msg.timestamp)

                except MySQLdb.Error as e:
                    logger.error(e)
                    logger.error(sqlcmd)
                    logger.error(msg.offset)
                    logger.error(msg.timestamp)
                    time.sleep(4)
                    sys.exit(1)

                except MySQLdb.Warning as e:
                    logger.error('%s: %s' % ('Warning', e))
                    logger.error(sqlcmd)



        elif msg_date.get('type') == 'DELETE':
            for l in msg_date.get('data'):
                sqlcmd = """DELETE FROM %s.%s where %s LIMIT 1;""" % (msg_date.get('database'), msg_date.get('table'), ' AND '.join(map(compare_items,
                                                                                                                                        l.items())))
                values = l.values()

                try:

                    cur.execute(sqlcmd,values)

                    with open(offset_log, 'w') as f:
                        offsetinfo = """servers:%s\ntopic:%s\noffset:%s\npartition:%s\ntimestamp:%s""" % (
                            kafka_server, kafka_topic, msg.offset, msg.partition, msg.timestamp)
                        f.write(offsetinfo)
                    db_connect.commit()

                    # if data Changed not equal to 1,target database data may not be consistent
                    if cur.rowcount != 1:
                        logger.error('The target instance does not have this rows ,the data may not be consistent')
                        logger.error(sqlcmd)
                        logger.error(values)
                        logger.error(msg.offset)
                        logger.error(msg.timestamp)


                except MySQLdb.Error as e:
                    logger.error(e)
                    logger.error(sqlcmd)
                    logger.error(msg.offset)
                    logger.error(msg.timestamp)
                    time.sleep(4)
                    sys.exit(1)

                except MySQLdb.Warning as e:
                    logger.error('%s: %s' % ('Warning', e))
                    logger.error(sqlcmd)



        elif msg_date.get('isDdl'):
            sqlcmd = msg_date.get('sql')
            db_connect = MySQLdb.connect(user=dbuser, passwd=dbpassword, host=host,port=dbport, charset="UTF8",db=msg_date.get('database'))
            cur = db_connect.cursor()
            try:

                cur.execute(sqlcmd)
                with open(offset_log, 'w') as f:
                    offsetinfo = """servers:%s\ntopic:%s\noffset:%s\npartition:%s\ntimestamp:%s""" % (
                        kafka_server, kafka_topic, msg.offset, msg.partition, msg.timestamp)
                    f.write(offsetinfo)
                db_connect.commit()
            except MySQLdb.Error as e:
                logger.error(e)
                logger.error(sqlcmd)
                logger.error(msg.offset)
                logger.error(msg.timestamp)
                time.sleep(4)
                sys.exit(1)

            except MySQLdb.Warning as e:
                logger.error('%s: %s' % ('Warning', e))
                logger.error(sqlcmd)




        elif re.match(r'create database|drop database', msg_date.get('sql'), re.M | re.I) and not msg_date.get('table'):
            sqlcmd = msg_date.get('sql')
            try:

                cur.execute(sqlcmd)
                with open(offset_log, 'w') as f:
                    offsetinfo = """servers:%s\ntopic:%s\noffset:%s\npartition:%s\ntimestamp:%s""" % (
                        kafka_server, kafka_topic, msg.offset, msg.partition, msg.timestamp)
                    f.write(offsetinfo)
                db_connect.commit()
            except MySQLdb.Error as e:
                logger.error(e)
                logger.error(sqlcmd)
                logger.error(msg.offset)
                logger.error(msg.timestamp)
                time.sleep(4)
                sys.exit(1)

            except MySQLdb.Warning as e:
                logger.error('%s: %s' % ('Warning', e))
                logger.error(sqlcmd)




def create_dire(p):
    if not os.path.exists(p):
        os.makedirs(p)


def log(logfile):
    logger = logging.getLogger()
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s %(levelname)-8s: %(message)s',
        filename=logfile,
        filemode='a')
    return logger


def parse_args():
    parser = argparse.ArgumentParser(description='dbsync:mysql multi-instance data synchronization   ', add_help=False)
    parser.add_argument('--help', action='help', help='show this help message and exit')
    parser.add_argument("-h", '--host', help="target database instance ip", type=str, default=False, required=False)
    parser.add_argument("-P", '--port', help="target database instance port default 3306", type=str, default=3306, required=False)
    parser.add_argument("-u", '--user', help="target database instance database user", type=str, default=False, required=False)
    parser.add_argument("-p", '--password', help="target database instance database user password", type=str, default=3306, required=False)
    parser.add_argument('-s', '--server', help="kafka server format 192.168.1.95:9092,192.168.1.96:9092,192.168.1.97:9092 or 192.168.1.95:9092",
                        type=str,
                        required=False)
    parser.add_argument("-t", '--topic', help="kafka topic", type=str, default=False, required=False)
    parser.add_argument("-g", '--group', help="kafka consumer group id", type=str, default=False, required=False)
    # args = parser.parse_args()
    return parser


def config_args(**args):
    if not args.get('host'):
        raise Exception('-h parameter is must requisite plase input this parameter')
    elif not args.get('server'):
        raise Exception('-s parameter is must requisite plase input this parameter')
    elif not args.get('topic'):
        raise Exception('-t parameter is must requisite plase input this parameter')
    elif not args.get('user'):
        raise Exception('-u parameter is must requisite plase input this parameter')
    elif not args.get('password'):
        raise Exception('-p parameter is must requisite plase input this parameter')






def term_sig_handler(signum, frame):


    # print 'catched singal: %d' % signum
    # print 'programmer is exiting ,place wating ......'
    # time.sleep(3)
    print 'programmer exited succeed'

    # sys.exit()



def craete_offset_file(dbsync_info,kafka_server,kafka_topic):
    if os.path.exists(dbsync_info):
        if not os.path.getsize(dbsync_info):
            with open(dbsync_info, 'w') as f:
                offsetinfo = """servers:%s\ntopic:%s\noffset:%s\npartition:%s\ntimestamp:%s""" % (
                    kafka_server, kafka_topic, 0, 0, 0)
                f.write(offsetinfo)
    else:
        with open(dbsync_info, 'w') as f:
            offsetinfo = """servers:%s\ntopic:%s\noffset:%s\npartition:%s\ntimestamp:%s""" % (
                kafka_server, kafka_topic, 0, 0, 0)
            f.write(offsetinfo)


if __name__ == '__main__':

    # for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM,signal.SIGQUIT,signal.SIGALRM]:
    #     signal.signal(sig, term_sig_handler)

    if not sys.argv[1:]:
        parse_args().print_help()
        sys.exit(1)
    else:
        args = parse_args().parse_args()
        config_args(host=args.host, server=args.server, topic=args.topic, user=args.user, password=args.password)

        if not args.group:
            kafka_group = args.topic + 'group'
        else:
            kafka_group = args.group


        dbuser = args.user
        dbpassword = args.password
        host = args.host
        dbport = args.port
        kafka_server = args.server
        kafka_topic = args.topic
        # create log directory
        create_dire('log')
        dbsync_info = """log/%s.info""" % (args.topic)

        craete_offset_file(dbsync_info,kafka_server,kafka_topic)


        logfile = """log/%s.log""" % (args.topic)
        logger = log(logfile)

        #get consumer offset
        with open(dbsync_info,'r') as f:
            offset_number = f.readlines()[2].strip().split(':')[1]

        #assign consumer offset
        consumer = kafka_consumer(kafka_group,kafka_server)
        partition = TopicPartition(kafka_topic, 0)

        consumer.assign([partition])
        consumer.seek(partition,int(offset_number) + 1)

        dbsync_run(consumer,dbsync_info,dbuser,dbpassword,host,dbport)
