    #!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"]="/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# In[3]:


spark=SparkSession.builder.appName("Kafka-to-HDFS").master("local").getOrCreate()
spark


# In[4]:


#Reading data from hdfs
df=spark.read.json("clickstream.json")


# In[5]:


df.show(10,truncate=False)


# In[6]:


#Selecting the columns from the clickstream data set
df=df.select(get_json_object(df['value_str'],"$.customer_id").alias("customer_id"),
            get_json_object(df['value_str'],"$.app_version").alias("app_version"),
            get_json_object(df['value_str'],"$.OS_version").alias("OS_version"),
            get_json_object(df['value_str'],"$.lat").alias("lat"),
            get_json_object(df['value_str'],"$.lon").alias("lon"),
            get_json_object(df['value_str'],"$.page_id").alias("page_id"),
            get_json_object(df['value_str'],"$.button_id").alias("button_id"),
            get_json_object(df['value_str'],"$.is_button_click").alias("is_button_click"),
            get_json_object(df['value_str'],"$.is_page_view").alias("is_page_view"),
            get_json_object(df['value_str'],"$.is_scroll_up").alias("is_scroll_up"),
            get_json_object(df['value_str'],"$.is_scroll_down").alias("is_scroll_down"),
            get_json_object(df['value_str'],"$.timestamp").alias("timestamp")
             )


# In[7]:


df.printSchema()


# In[8]:


df.show(10)


# In[10]:


#Writing the dataset to hdfs
df.coalesce(1).write.format('csv').mode('overwrite').save('/user/root/clickstream_flattened',header='true')


# In[ ]:
