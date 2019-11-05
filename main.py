import configparser
from pymongo import MongoClient, InsertOne, UpdateOne, errors
import os
import logging
import logs

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "/config.ini")

# create logger
logger = logging.getLogger('abc')

# configure logging
logs.configure_logging()


def connect_mongo(target_db):
    """
    Creates a mongo connection
    :return: mongo connection to a particular db or on exception False.
    """
    try:
        client = MongoClient('mongodb://' + config.get(target_db, 'USER') + ':' + config.get(target_db, 'PWD') + '@' +
                             config.get(target_db, 'HOST') + '/' + config.get(target_db, 'AUTHDB')
                             + '?readPreference=primary', connect=False)
        # client = MongoClient("mongodb://localhost:27017/")
        connection = client[config.get(target_db, 'DB')]
        return connection
    except Exception as e:
        print('Error in connecting to mongo : {}'.format(e))
        return False


def get_collection():
    _collection = (col for col in src_db.collection_names())
    return _collection


def execute_bulk(db, operations, c):
    try:
        db.bulk_write(operations, ordered=False)

    except errors.DuplicateKeyError as e:
        print(f'Execute Bulk Duplicate Key Error {e}')
        return True

    except errors.BulkWriteError as bwe:
        print(f'Execute Bulk BulkWriteError {bwe.details}')
        panic = filter(lambda x: x['code'] != 11000, bwe.details['writeErrors'])

        if len(list(panic)) > 0:
            print("Execute Bulk panic situation")
            return False
        else:
            print("Execute Bulk says don't panic")
            logger.info(f"Execute Bulk Some Duplicates found for collection {c}")
            return True

    except Exception as e:
        print(f'Execute Bulk General exception {e} for {c}')
        return False


def log_bulk(db, log_operations, c):
    try:
        db.bulk_write(log_operations, ordered=True)

    except errors.DuplicateKeyError as e:
        print(f'Duplicate Key Error {e}')
        return True

    except errors.BulkWriteError as bwe:
        print(f'BulkWriteError {bwe.details}')
        panic = filter(lambda x: x['code'] != 11000, bwe.details['writeErrors'])

        if len(list(panic)) > 0:
            print("really panic")
            return False
        else:
            print("don't panic")
            logger.info(f"Some Duplicates found for collection {c}")
            return True

    except Exception as e:
        print(f'General exception {e} for {c}')
        return False


def save(c, destination_db):
    print(f"Saving started for {c}")
    destination_collection = destination_db[c]
    source_colection = src_db[c]
    data = source_colection.find({"done": {"$exists": False}}, {'_id': 0})
    bulk_insert_operations = list()
    bulk_log = list()

    for d in data:
        bulk_insert_operations.append(InsertOne(d))
        bulk_log.append(UpdateOne({'iprd': d['iprd'], 'dt': d['dt']},
                                  {'$set': {'done': True}}
                                  ))
        if len(bulk_insert_operations) == 800:
            execute_bulk(destination_collection, bulk_insert_operations, c)
            log_bulk(source_colection, bulk_log, c)
            bulk_log = list()
            bulk_insert_operations = list()

    if len(bulk_insert_operations):
        # if length of bulk operation is less than limit of 800
        execute_bulk(destination_collection, bulk_insert_operations, c)
        log_bulk(source_colection, bulk_log, c)

    source_colection.rename(c + "_done")
    logger.info(f"Saving done for {c}")
    print(f"Saving done for {c}")
    return True


if __name__ == '__main__':
    # source db
    source_db = 'MONGO_NEW'
    src_db = connect_mongo(source_db)

    # destination db where data is to be saved.
    target_db = connect_mongo('MONGO_OLD')

    # generator object containing collection names.
    collections = get_collection()

    while True:
        try:
            # get collection.
            collection = collections.__next__()
            if '_done' in collection:
                print('Collection already completed', collection)
                continue

            save(collection, target_db)

        except StopIteration:
            logger.info('Completed all collections. Exiting [x][x][x]')
            break
        except Exception as e:
            print(e)
            logger.error(e)









