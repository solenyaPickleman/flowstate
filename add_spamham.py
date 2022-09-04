'''
spark-submit --master local[7] --driver-memory 32g --packages org.postgresql:postgresql:42.4.0,com.johnsnowlabs.nlp:spark-nlp-gpu_2.12:4.1.0 add_spamham.py 
'''

import sparknlp

from pyspark.sql import SQLContext 
from pyspark.ml.pipeline import  PipelineModel
from pyspark.sql.functions import col 

#setup spark / variables 
spark = sparknlp.start(gpu=True) 
sqlContext = SQLContext(spark.sparkContext)
sql = sqlContext.sql #get data! 

pipeline = PipelineModel.load('/home/user/models/hamspam-enron/')

print('\n\nMODEL LOADED INFERENCE STARTING\n\n')

df = spark.read.format("jdbc").option("url", 'jdbc:postgresql://localhost:5432/emails').option('driver','org.postgresql.Driver').option("query", "select id, body as text from emails").option("user", "flow").option("password", "password").option('fetchsize', '1000').load().repartition(240)

annotations = pipeline.transform(df).select('id', (col('category.result')[0] == 'spam' ).alias('is_spam')) 
annotations.write.jdbc(url='jdbc:postgresql://localhost:5432/emails', table='is_spam', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'flow','password': 'password'})

annotations.createOrReplaceTempView('spam')
sql('select  sum (cast ( is_spam as int) ) / count(*) as ratio  from spam ').show() 

