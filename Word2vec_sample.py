from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, DoubleType
from pyspark.ml.feature import Word2Vec, Word2VecModel 
import time
from functools import reduce

import pandas as pd
import math

def GC_func(i):
    # Fetch top 100 Haloed GC
    GC_res = model.findSynonyms(i, 100).select("word", fmt("similarity", 100).alias("similarity"))
    
    # Convert to pandas df
    pandas_df = GC_res.select("*").toPandas()
    
    # Filter condition
    pandas_df['similarity'] = pd.to_numeric(pandas_df['similarity'])
    
    if((pandas_df['similarity'] >= 0.9).all()):
        pandas_df = pandas_df.loc[pandas_df['similarity'] >= 0.9] 
    else:
        if(len(pandas_df.loc[pandas_df['similarity'] >= 0.9]) <= 25):
            pandas_df = pandas_df.head(25)
        else:
            pandas_df = pandas_df.loc[pandas_df['similarity'] >= 0.9]  
    pandas_df['Focus_GC'] = i
    return pandas_df

def combinepandas_df(data_list):
    #print(data_list)
    result = pd.concat(data_list)
    
    # Convert back to pyspark df
    schema = StructType([StructField('word', StringType(), True),
                     StructField('similarity', StringType(), True),
                     StructField('Focus_GC', StringType(), True)])
    GC_res = spark.createDataFrame(result, schema=schema)
    df_comp = GC_res.selectExpr("Focus_GC as Focus_GC", "word as Halo_GC", "similarity as Cos_Sim")
    return GC_res

documentDF = spark.createDataFrame([
        ("Hi I heard about Spark".split(" "), ),
        ("I wish Java could use case classes".split(" "), ),
        ("Logistic regression models are neat".split(" "), )
    ], ["text"])

documentDF.show()

word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result")
model = word2Vec.fit(documentDF)

GC_new_list = [row.word for row in model.getVectors().select('word').collect()]

batchsize = 500
start = time.time()
for n in range(0, len(GC_new_list), batchsize):
    flag = 0
    if(len(GC_new_list) - (n+batchsize) < batchsize):
        GC_batch_list = GC_new_list[n:len(GC_new_list)]
        list_of_df = map(GC_func,GC_batch_list)
        df_batch_comp = combinepandas_df(list(list_of_df))
        break

    GC_batch_list = GC_new_list[n:n+batchsize]
    list_of_df = map(GC_func,GC_batch_list)
    
    df_batch_comp = combinepandas_df(list_of_df)
    print(df_batch_comp)
    
df_batch_comp.show()

''' #RESULT BELOW:
+----------+--------------------+--------+
|      word|          similarity|Focus_GC|
+----------+--------------------+--------+
|      neat|  0.7562598586082451|   heard|
|     Spark| 0.47184371948242193|   heard|
|      Java| 0.44274002313613886|   heard|
|      wish| 0.43074005842208873|   heard|
|  Logistic|  0.4146628975868225|   heard|
|      case| 0.35891056060790993|   heard|
|     about|-0.03944322466850282|   heard|
|         I|-0.06950964033603671|   heard|
|       are|-0.26707756519317605|   heard|
|       use|  -0.796978116035461|   heard|
|    models| -0.8080761432647704|   heard|
|        Hi| -0.8166412115097041|   heard|
|   classes| -0.9024752378463741|   heard|
|     could| -0.9553157091140749|   heard|
|regression| -0.9877846837043759|   heard|
|      case|  0.7865696549415586|     are|
|      wish|  0.5828941464424126|     are|
|        Hi|  0.5786922574043274|     are|
|     about|  0.5454659461975097|     are|
|       use| 0.23391805589199055|     are|
+----------+--------------------+--------+
only showing top 20 rows
'''