# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 13:57:05 2023

@author: John Torres
"""
#PubNub imports
from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory, PNOperationType
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

#Utility imports
import pandas as pd
import time
from csv import DictWriter

#PySpark/PyArrow imports
import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pyarrow as pa
import pyarrow.parquet as pq


spark = SparkSession.builder.master("local").appName("pyspark_stream_setup").getOrCreate()
    
pubnubWiki_Schema = StructType() \
                    	.add("event", "string")\
                    	.add("item", "string")\
                    	.add("link", "string")\
                    	.add("user", "string")

#PubNub config
pnconfig = PNConfiguration()
pnconfig.subscribe_key = 'sub-c-83a959c1-2a4f-481b-8eb0-eab514c06ebf'
pnconfig.publish_key = 'pub-c-8429e172-5f05-45c0-a97f-2e3b451fe74f'
pnconfig.user_id = "my_custom_user_id"
pnconfig.connect_timeout = 5
pubnub = PubNub(pnconfig)

#primekeys is a collection of keys that are present in each tweet, used in subscribe callback message
wikikeys = ['event', 'item', 'link', 'user']

class MySubscribeCallback(SubscribeCallback):

    def status(self, pubnub, status):
        pass
        # PubNub setup
        if status.operation == PNOperationType.PNSubscribeOperation \
                or status.operation == PNOperationType.PNUnsubscribeOperation:
            if status.category == PNStatusCategory.PNConnectedCategory:
                pass
                # Is no error or issue whatsoever
            elif status.category == PNStatusCategory.PNReconnectedCategory:
                pass
                # If subscribe temporarily fails but reconnects. This means
                # there was an error but there is no longer any issue
            elif status.category == PNStatusCategory.PNDisconnectedCategory:
                pass
                # No error in unsubscribing from everything
            elif status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
                pass
                # This is an error, retry will be called automatically
            elif status.category == PNStatusCategory.PNAccessDeniedCategory:
                pass
                # This means that Access Manager does not allow this client to subscribe to this
                # channel and channel group configuration. This is another explicit error
            else:
                pass
                # This is usually an issue with the internet connection, this is an error, handle appropriately
                # retry will be called automatically
        elif status.operation == PNOperationType.PNSubscribeOperation:
            # Heartbeat operations can in fact have errors, so it is important to check first for an error.
            # For more information on how to configure heartbeat notifications through the status
            # PNObjectEventListener callback, consult <link to the PNCONFIGURATION heartbeart config>
            if status.is_error():
                pass
                # There was an error with the heartbeat operation, handle here
            else:
                pass
                # Heartbeat operation was successful
        else:
            pass
            # Encountered unknown status type
 
    def presence(self, pubnub, presence):
        pass  # handle incoming presence data
    def message(self, pubnub, message):
        res = dict()
        for key, val in message.message.items():
            if key in wikikeys:
                res[key] = val
        with open('p4output.csv', 'a', encoding="utf-8") as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames=wikikeys)
            dictwriter_object.writerow(res)
            f_object.close()
        pass  # handle incoming message data
        
subscribeCallback = MySubscribeCallback()
pubnub.add_listener(subscribeCallback)
pubnub.subscribe().channels('pubnub-wikipedia').execute()

#Increase time.sleep from (1) to higher number to increase amount of data gathered to .csv
time.sleep(10)
pubnub.unsubscribe_all()

#Read in .csv
pubnubwikidf = pd.read_csv('p4output.csv', names=wikikeys)
print("\nShape is", pubnubwikidf.shape)

# Convert pandas DataFrame to PyArrow Table
table = pa.Table.from_pandas(pubnubwikidf)

# Write the PyArrow Table to Parquet format
pq.write_table(table, 'data.parquet')

# Create a SparkSession object
spark = SparkSession.builder.appName('PandasToSparkDF').getOrCreate()

# Read the Parquet file into a PySpark DataFrame
df_spark = spark.read.parquet('data.parquet')

# Show the PySpark DataFrame
df_spark.show()