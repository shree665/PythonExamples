from flask import Flask, request
from flask_restful import Resource, Api
from json import dumps
import elasticsearch
import curator
import sys
import smtplib
import os
import time
import schedule
import threading
import sys
import traceback

app = Flask(__name__)
api = Api(app)

localhost = False
localtime = time.asctime( time.localtime(time.time()) )

if localhost:
    expiration_date = 1
    es_connection = "http://localhost:9200"
    interval_time = 5
else:
    expiration_date = os.getenv("EXPIRATION_DATE")
    es_connection = os.getenv("ES_CONNECTION")
    interval_time = os.getenv("DO_POLL_TIME")
    doDelete = os.getenv("DO_DELETE")



print "Purging data older than " + str(expiration_date) + " days "


class CuratorListener(object):

    def __init__(self, interval=int(interval_time)):

        self.interval = interval

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True
        thread.start()

    def run(self):
        while True:
            purge_es()
            time.sleep(self.interval)

def purge_es():

    client = elasticsearch.Elasticsearch(es_connection)
    ilo = curator.IndexList(client)
    print("*********************** ")
    print("*****All Indices:****** ")
    for idx in ilo.indices:
        print(idx)
    try:

        ilo.filter_by_regex(kind='suffix', value='-stream')
        ilo.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d', unit='days', unit_count=int(expiration_date))
        delete_indices = curator.DeleteIndices(ilo)
        expiringIndices = ilo.indices
        print("*********************** ")
        print("***Expiring Indices:*** ")
        for idx in ilo.indices:
            print(idx)

        if doDelete=="true":
            print("Deleting indices")
            delete_indices.do_action()
        else:
            print("Dry run no indices will be deleted")
            delete_indices.do_dry_run()

    except Exception as e:
        print("*********************** ")
        print("***Expiring Indices:*** ")
        print("No indices were found matching the search criteria ")
        print(e)
        

class Purge(Resource):
    def get(self):
        client = elasticsearch.Elasticsearch(es_connection)
        ilo = curator.IndexList(client)
        print("All Indices:")
        for idx in ilo.indices:
            print(idx)
        try:


            #ilo.filter_by_regex(kind='suffix', value='-stream')
            ilo.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d', unit='days', unit_count=int(expiration_date))
            delete_indices = curator.DeleteIndices(ilo)
            delete_indices.do_dry_run()
            expiringIndices = ilo.indices
            expiringIndices= map(lambda a: str(a), expiringIndices)
            print("Expiring Indices:")
            for idx in ilo.indices:
                print(idx)
        except e:
            print(" ***No inidices matching the search criteria were found*** ")


class HealthCheck(Resource):
    def get(self):
        return "I'm healthy"




api.add_resource(Purge, '/purge')
api.add_resource(HealthCheck, '/_healthcheck')



if __name__ == '__main__':
    CuratorListener()
    app.run(debug=True, host='0.0.0.0')