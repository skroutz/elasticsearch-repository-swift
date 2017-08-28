import os, os.path, sys
import json
import tempfile
import logging
import time

from swiftclient.service import SwiftService, SwiftError
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

es = Elasticsearch(hosts=['http://elasticsearch:9200'])


def await_elasticsearch(es_client):
    if not es_client.ping():
        wait = 5
        while wait < 60:
            time.sleep(wait)
            if es_client.ping():
                break
            wait *= 2

    if not es_client.ping():
        raise Exception("Error connecting to Elasticsearch, quitting")


def auth_swift():
    from keystoneauth1 import session
    from keystoneauth1.identity import v3

    # Create a password auth plugin
    auth = v3.Password(auth_url='http://127.0.0.1:5000/v3/',
                       username='tester',
                       password='testing',
                       user_domain_name='Default',
                       project_name='Default',
                       project_domain_name='Default')

    # Create session
    keystone_session = session.Session(auth=auth)

    # Create swiftclient Connection
    # swift_conn = Connection(session=keystone_session)


def connect_swift():
    container = 'backup'
    minimum_size = 10*1024**2
    with SwiftService(options={'tenant_id': 'foo','tenant_name': 'foo'}) as swift:
        try:
            list_parts_gen = swift.list(container=container)
            for page in list_parts_gen:
                if page["success"]:
                    for item in page["listing"]:

                        i_size = int(item["bytes"])
                        if i_size > minimum_size:
                            i_name = item["name"]
                            i_etag = item["hash"]
                            print(
                                "%s [size: %s] [etag: %s]" %
                                (i_name, i_size, i_etag)
                            )
                else:
                    raise page["error"]
            return swift

        except SwiftError as e:
            logger.error(e.value)


if __name__ == '__main__':
    auth_swift()
    swift_client = connect_swift()
    if swift_client is not None:
        print("Waiting for Elasticsearch to become available....")
        time.sleep(20)
        await_elasticsearch(es)

        print("Elasticsearch available; making sure Swift is too")

        snapshot_body = {
            "type": "url",
            "settings": {
                "url":  "http://download.elasticsearch.org/definitiveguide/sigterms_demo/"
            }
        }
        es.snapshot.create_repository(repository='test', body=snapshot_body)
        es.snapshot.create(repository='test', snapshot='my_snapshot')

    # index_body = {
    #     "indices": "index_1,index_2"
    # }
    # es.snapshot.create(repository='test', snapshot='my_snapshot', body=index_body)



