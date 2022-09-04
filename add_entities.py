'''
spark-submit --master local[7] --driver-memory 32g --packages org.postgresql:postgresql:42.4.0,com.johnsnowlabs.nlp:spark-nlp-gpu_2.12:4.1.0 add_entites.py
'''

import sparknlp

from pyspark.sql import SQLContext 
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import explode, arrays_zip, col , expr, collect_list, rand

#setup spark / variables 
spark = sparknlp.start(gpu=True) 
sqlContext = SQLContext(spark.sparkContext)
sql = sqlContext.sql #get data! 

pipeline = PretrainedPipeline("xlm_roberta_large_token_classifier_hrl_pipeline", lang = "xx")

df = spark.read.format("jdbc").option("url", 'jdbc:postgresql://localhost:5432/emails').option('driver','org.postgresql.Driver').option("query", "select id, body as text from emails").option("user", "flow").option("password", "password").option('fetchsize', '1000').load().repartition(12500)

#run the annotations, and filter the results to be only the rows needed later on 
annotations = pipeline \
    .annotate(df, 'text') \
    .select('id', 'finished_ner_chunk', expr("filter(finished_ner_chunk_metadata, row -> row._1 == 'entity' )")['_2'] \
        .alias('entity_type')) 

annotations = annotations.withColumn("new", arrays_zip('finished_ner_chunk', 'entity_type')) \
       .withColumn("new", explode("new"))\
       .select("id",  'new.finished_ner_chunk', "new.entity_type")

annotations.write.jdbc(url='jdbc:postgresql://localhost:5432/emails', table='temp_entities', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'flow','password': 'password'})

#  grab that temp table flip the schema so its id, persons , orgs, .... 

df = spark.read.format("jdbc").option("url", 'jdbc:postgresql://localhost:5432/emails').option('driver','org.postgresql.Driver').option("query", "select id, finished_ner_chunk, entity_type from temp_entities").option("user", "flow").option("password", "password").option('fetchsize', '1000').load().repartition(12500, 'id' )

df.groupby('id').pivot('entity_type').agg(collect_list('finished_ner_chunk')).write.jdbc(url='jdbc:postgresql://localhost:5432/emails', table='entities', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'flow','password': 'password'})

# # 

# results = annotations.groupby('id').pivot('entity_type').select(collect_list('finished_ner_chunk'))

# annotations.write.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='email_entities', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})

