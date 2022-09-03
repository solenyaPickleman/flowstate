'''
pyspark --master local[6] --driver-memory 12g --jars /home/computron/jars/postgresql-42.4.0.jar --packages com.johnsnowlabs.nlp:spark-nlp_2.12:4.0.2
'''

from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.sql.functions import substring, col 

pipeline = PipelineModel.load('/home/computron/models/enron_spam_ham/')

#get data! 

pgsql = sqlContext.read.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='emails', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})
pgsql.createOrReplaceTempView('emails')

df = sql('select id, body as text  from emails').repartition(200)

annotations = pipeline.transform(df).select('id', (col('category.result')[0] == 'spam' ).alias('is_spam')) 

annotations.write.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='email_isspam', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})

# annotations.createOrReplaceTempView('spam')

# sql('select  sum (cast ( is_spam as int) ) / count(*) as ratio  from spam ').show() 


