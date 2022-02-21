# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import split, isnan, count, when, first, col, countDistinct

spark = SparkSession.builder.getOrCreate()

path = "MS1.txt"

df = spark.read.option("lineSep", "\n").text(path)

#%%

#Renaming & dropping columns, Price to float

df = df.withColumn("Name",split("value", ",").getItem(0))
df = df.withColumn("Date",split("value",",").getItem(1))
df = df.withColumn("Price",split("value",",").getItem(2))
df = df.withColumn("Volume",split("value",",").getItem(3))
df = df.drop("value", "Volume")
df = df.withColumn("Price",col("Price").cast("float"))
df = df.limit(100000)

#%%

#Transforming dataframe to dataframe with 1 row is 1 date and 1 column = 1 Stock

df2 = df.groupby("Date").pivot("Name").avg("Price")
df2 = df2.orderBy("Date")

df2.show(10, False)

