from pyspark import SparkConf, SparkContext
import collections
from collections import namedtuple
import csv
from collections import deque
import pandas as pd
import numpy  as np 
from pyspark.sql import SparkSession
from pyspark.sql import Row
from itertools import islice
from pyspark.sql.functions import udf
from pyspark.sql.types import *


spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL1").getOrCreate()
event_dict={}

def mapper(line):
    fields = line.split(',')
    return Row(event_id=str(fields[0].encode("utf-8")), collector_tstamp=str(fields[1].encode("utf-8")), domain_userid=str(fields[2].encode("utf-8")),page_urlpath=str(fields[3].encode("utf-8")))


def create_eventdict(page_view):
	for val in page_view.collect():
		u_id=val[2]
		#print(u_id)
		event_id=val[0]
		if u_id not in event_dict:
			event_dict[u_id]=deque([event_id])
			event_dict[u_id].append("Null")
			event_dict[u_id].popleft()
		else:
			right_element=event_dict[u_id].pop()
			event_dict[u_id].append(event_id)
			event_dict[u_id].append(right_element)


def get_event(u_id):
	return event_dict[u_id].popleft()

	
df = spark.read.csv("C:/Spark_Files/friends/pageViews.csv", sep =",", header=True )
newdf=df.sort("domain_userid","collector_tstamp").coalesce(1).cache()

df.write.csv("mycsv_pre.csv",header=True)
print("**********************************************************")
print(type(newdf))
print("**********************************************************")

create_eventdict(newdf)
udfValueToEvent = udf(get_event, StringType())
df_with_cat = df.withColumn("Next_event_id", udfValueToEvent("domain_userid"))
#df_with_cat.show(10)
 #"counts_.sort_values('num',ascending=False)
df_with_cat.write.csv("mycsv_post.csv",header=True)
#df.write.csv('path_filename of csv',header=True) ###yes still in partitions
"""
for k,v in event_dict.items():
	print(k,v)
"""
