#!/usr/bin/env python
# coding: utf-8

# In[111]:


import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"]="/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[112]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# In[113]:


spark=SparkSession.builder.appName("datewise_bookings_aggregates_spark").master("local").getOrCreate()
spark


# In[114]:


#Reading data from HDFS
df=spark.read.csv("/user/root/cab_rides/part-m-00000")


# In[115]:


df.show(10)


# In[116]:


df.printSchema()


# In[117]:


#The count of the dataset
df.count()


# In[118]:


#Renaming the columns 
new_col = ["booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat",
          "drop_lon","pickup_timestamp","drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver",
          "rating_by_customer","passenger_count"]


# In[119]:


new_df = df.toDF(*new_col)


# In[120]:


new_df.show(truncate=False)


# In[121]:


#Converting pickup_timestamp to date by extracting date from pickup_timestamp for aggregation
new_df=new_df.select("booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat",
          "drop_lon",to_date(col('pickup_timestamp')).alias('pickup_date').cast("date"),"drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver",
          "rating_by_customer","passenger_count")


# In[122]:


new_df.show()


# In[123]:


#Aggregation on pickup_date
agg_df=new_df.groupBy("pickup_date").count().orderBy("pickup_date")


# In[124]:


agg_df.show()


# In[125]:


#The count of Bookings aggregates_table
agg_df.count()


# In[107]:


agg_df.coalesce(1).write.format('csv').mode('overwrite').save('/user/root/datewise_bookings_agg',header='true')


# In[ ]:




