#install elasticsearch python connector first
# pip install elasticsearch

# Following Pythong script is to generate random Geospetial data for testing. It Generates GeoPoints randomly from 0 to -180 with 100 more attributes
# and inserts data to ElasticSearch using batch ElasticSearch API. I used to use these data to display points map using GeoServer

# Generate the documents for the index
import uuid
import random
import string
import requests
import json
from random import uniform
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from random import randrange
from datetime import timedelta
import time


#adjust url accordingly 
base_url = "elasticsearch_url"
es_index = "elasticsearch_index"
es_type = "elasticsearch_doc_type"
totalTimes = 500
bulkLoadRecords = 2000

#random word site
WORDS = requests.get("http://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain").content.splitlines()

x = 0
words = []
for w in WORDS:
    value = str(w).replace("b'","").replace("'", "")
    words.append(value)
    x += 1
s_nouns = ["A dude", "My mom", "The king", "Some guy", "A cat with rabies", "A sloth", "Your homie", "This cool guy my gardener met yesterday", "Superman"]
p_nouns = ["These dudes", "Both of my moms", "All the kings of the world", "Some guys", "All of a cattery's cats", "The multitude of sloths living under your bed", "Your homies", "Like, these, like, all these people", "Supermen"]
s_verbs = ["eats", "kicks", "gives", "treats", "meets with", "creates", "hacks", "configures", "spies on", "retards", "meows on", "flees from", "tries to automate", "explodes"]
p_verbs = ["eat", "kick", "give", "treat", "meet with", "create", "hack", "configure", "spy on", "retard", "meow on", "flee from", "try to automate", "explode"]
infinitives = ["to make a pie.", "for no apparent reason.", "because the sky is green.", "for a disease.", "to be able to make toast explode.", "to know more about archeology."]

d1 = datetime.strptime('1/1/2000 1:30 PM', '%m/%d/%Y %I:%M %p')
d2 = datetime.strptime('1/1/2019 4:50 AM', '%m/%d/%Y %I:%M %p')

print("Setting up connection...")
es = Elasticsearch([{'host': 'host_url', 'port': 9200}])
print('connected!!!')


def createMapping():
    mapping = '''
    {  
      "mappings":{ 
        "doc_type":{
          "properties":{  
            "geometry":{  
              "type":"geo_shape"
            }
          }
        }
      }
    }'''
    es.indices.create(index='doc_index', ignore=400, body=mapping)
    
def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)
    
def do_bulkLoad():
    j = 0
    actions = []
    while (j <= bulkLoadRecords):
        randomInt = random.randint(100,25485)
        xPos = round(uniform(-180,180)*1000)/1000
        yPos = round(uniform(-90,90)*1000)/1000
        action = {
            "_index": "subedi-1million",
            "_type":  "subedi-1million_type",
            "_source":{
                "geometry" : {
                    "coordinates" : [
                         xPos,
                         yPos
                    ],
                "type" : "Point"
                },
                "type": "Feature",
                "geometry_wkt" : "POINT (" + str(xPos) + ' ' + str(yPos) + ")",
                "properties": {
                    "DescriptionPoints" : "Random (" + str(xPos) + ' ' + str(yPos) + ")", 
                    "CurrentSpeed" : "145 miles/hr",
                    "Latitude" :  str(yPos),
                    "Longitude" : str(xPos),
                    "numberField": random.randint(1,1000000),
                    "randomDate": random_date(d1, d2),
                    "ID1": words[randomInt -1],
                    "ID2": words[randomInt -2],
                    "ID3": words[randomInt -3],
                    "ID4": words[randomInt -4],
                    "ID5": words[randomInt -5],
                    "ID6": words[randomInt -6],
                    "ID7": words[randomInt -7],
                    "ID8": words[randomInt -8],
                    "ID9": words[randomInt -9],
                    "ID10": words[randomInt -10],
                    "ID11": words[randomInt -1],
                    "ID12": words[randomInt -2],
                    "ID13": words[randomInt -3],
                    "ID14": words[randomInt -4],
                    "ID15": words[randomInt -5],
                    "ID16": words[randomInt -6],
                    "ID17": words[randomInt -7],
                    "ID18": words[randomInt -8],
                    "ID19": words[randomInt -9],
                    "ID20": words[randomInt -10],
                    "ID21": words[randomInt -1],
                    "ID22": words[randomInt -2],
                    "ID23": words[randomInt -3],
                    "ID24": words[randomInt -4],
                    "ID25": words[randomInt -5],
                    "ID26": words[randomInt -6],
                    "ID27": words[randomInt -7],
                    "ID28": words[randomInt -8],
                    "ID29": words[randomInt -9],
                    "ID30": words[randomInt -10],
                    "ID31": words[randomInt -1],
                    "ID32": words[randomInt -2],
                    "ID33": words[randomInt -3],
                    "ID34": words[randomInt],
                    "ID35": words[randomInt],
                    "description1": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description2": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description3": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]), 
                    "description4": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description5": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description6": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description7": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description8": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description9": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description10": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]), 
                    "description11": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description12": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description13": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description14": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description15": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description16": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description17": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description18": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description19": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description20": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]), 
                    "description21": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description22": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description23": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description24": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description25": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description26": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description27": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]), 
                    "description28": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description29": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description30": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description31": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description32": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description33": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description34": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description111": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description211": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description311": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]), 
                    "description411": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description511": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description611": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description711": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description811": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description911": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description101": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]), 
                    "description1111": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description121": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description131": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description141": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description151": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description161": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description171": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description181": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description191": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description201": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]), 
                    "description2111": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description221": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description231": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description241": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description251": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description261": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description271": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]), 
                    "description281": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description291": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description301": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description3111": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description321": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description331": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)]),
                    "description341": ''.join([random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(p_nouns).lower(), random.choice(infinitives)])
                }
             }
            }
        actions.append(action)
        j += 1
    return actions
    
#creating mapping first only for geometry
createMapping()
print("mapping is created")

#looping through the number records that many needs to create
wholeStart = time.time()
for i in range(totalTimes):
    actions = do_bulkLoad()
    helpers.bulk(es, actions, chunk_size=bulkLoadRecords)
wholeQueryTime = time.time() - wholeStart
print('Query time for 1 million records  is {} seconds'.format(wholeQueryTime))