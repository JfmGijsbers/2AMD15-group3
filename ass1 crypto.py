# -*- coding: utf-8 -*-
"""
Created on Sun Feb 20 13:43:53 2022

@author: Stan
"""

from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.functions import split, isnan, count, when, first, col, to_date, lit

spark = SparkSession.builder.getOrCreate()

path = "Crypto-data.csv"

df = spark.read.csv(path, header=True)

#%%

# Dropping columns, Price to float, String Date to Date and only dates between 2016&2020

df = df.drop("Open","High","Low","Volume","Market Cap") 
df = df.withColumn("Close",col("Close").cast("float"))
df = df.withColumn("Date", to_date(col("Date"),"yyyy-MM-dd"))
df = df.where("Date > '2015-12-31'")
df = df.where("Date < '2021-01-01'")

crypto = df.select('Symbol').distinct().count() #number of distinct coins

#%%

#Transforming dataframe to dataframe with 1 row is 1 date and 1 column = 1 coin

df2 = df.groupby("Date").pivot("Symbol").avg("Close")
df2 = df2.orderBy("Date")
df2.show(5,False)

#%%

df2=df2.drop("WHALE") #WHALE coin gave difficulties :(

#counting number of null values for every coin

df_null = df2.select([count(when(col(i).isNull(),i)).alias(i) for i in df2.columns])