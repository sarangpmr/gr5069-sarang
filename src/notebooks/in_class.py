# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

for i in range(1,1000):
    if (i % 2 == 0) or (i % 3 == 0) or (i % 5 == 0):
        i = "W"
    else:
        print(i)
