# Databricks notebook source
# Import all packages
import pyspark.sql.functions as pssf
import pyspark.sql.types as psst

# COMMAND ----------

# MAGIC %md #### Read in Dataset

# COMMAND ----------

# MAGIC %md ##### E - Extraction of Data

# COMMAND ----------

# Use Spark to read a .csv file that is in our S3 bucket

# df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv')
df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header = True)
    # ALTERNATIVES: 
        # df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv')
            # this will not associate the header row to columns
        # df_laptimes = spark.read.format("csv").option("header","true").load('s3://columbia-gr5069-main/raw/lap_times.csv')

# COMMAND ----------

display(df_laptimes) # instead of df_laptimes.head()

# COMMAND ----------

df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header = True)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md ##### T - Transform Data

# COMMAND ----------

# Take current_date() - driver dob and divide by 365 to get age in terms of years
df_drivers = df_drivers.withColumn("age"
                                   , pssf.datediff(pssf.current_date(), df_drivers.dob)/365)

# Take driver age in years (double type) and convert to int (i.e. taking the floor of age)
df_drivers = df_drivers.withColumn("age", df_drivers['age'].cast(psst.IntegerType()))

# COMMAND ----------

display(df_drivers)

# Note: if you want to take the drivers' true age (e.g. for those whose age is listed as really old, like 100+), you 
# can create a UDF to combine a dataset of drivers' date of death

# COMMAND ----------

df_lap_drivers = df_drivers.join(df_laptimes, on = ['driverId'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

df_lap_drivers = df_drivers.select('driverId', 'driverRef', 'code', 'forename', 'surname', 'nationality', 'age').join(df_laptimes, on = ['driverId'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# Load in Races dataset to get raceId names
df_races = spark.read.csv('s3://columbia-gr5069-main/raw/races.csv', header = True)

# COMMAND ----------

display(df_races)

# COMMAND ----------

df_lap_drivers = df_lap_drivers.join(df_races.select('year', 'name', 'raceId'), on = ['raceId'])

# COMMAND ----------

df_lap_drivers = df_drivers.select('driverId', 'driverRef', 'code', 'forename', 'surname', 'nationality', 'age').join(df_laptimes, on = ['driverId'])
df_lap_drivers = df_lap_drivers.join(df_races.select('year', 'name', 'raceId'), on = ['raceId'])

display(df_lap_drivers)

# COMMAND ----------

df_lap_drivers = df_lap_drivers.drop('raceId', 'driverId').select('year'
                                                                  , 'name'
                                                                  , 'driverRef'
                                                                  , 'code'
                                                                  , 'forename'
                                                                  , 'surname'
                                                                  , 'nationality'
                                                                  , 'age'
                                                                  , 'lap'
                                                                  , 'position'
                                                                  , 'time'
                                                                  , 'milliseconds')
display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md ##### Aggregate by Age

# COMMAND ----------

df_agg_age = df_lap_drivers.groupby('age').agg(pssf.avg('milliseconds'))

# COMMAND ----------

display(df_agg_age)

# COMMAND ----------

# MAGIC %md #### L - Load Data into S3

# COMMAND ----------

# store dataset into our S3/bucket/processed folder
    # copy S3 folder URL
    # create a new folder in which to store the file
df_agg_age.write.csv('s3://smp2249-gr5069/processed/inclass/laptimes_by_age.csv')
