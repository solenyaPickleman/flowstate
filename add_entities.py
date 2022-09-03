'''
pyspark --master local[6] --driver-memory 12g --jars /home/computron/jars/postgresql-42.4.0.jar --packages com.johnsnowlabs.nlp:spark-nlp_2.1\2:4.0.2
'''

import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import explode, arrays_zip, col , expr, collect_list

sparknlp.start()

pipeline = PretrainedPipeline("xlm_roberta_large_token_classifier_hrl_pipeline", lang = "xx")


pgsql = sqlContext.read.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='emails', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})
pgsql.createOrReplaceTempView('emails')

df = sql('select id, body as text  from emails ').repartition(2500)

#run the annotations, and filter the results to be only the rows needed later on 
annotations = pipeline \
    .annotate(df, 'text') \
    .select('id', 'finished_ner_chunk', expr("filter(finished_ner_chunk_metadata, row -> row._1 == 'entity' )")['_2'] \
        .alias('entity_type')) 

annotations = annotations.withColumn("new", arrays_zip('finished_ner_chunk', 'entity_type')) \
       .withColumn("new", explode("new"))\
       .select("id",  'new.finished_ner_chunk', "new.entity_type")

# flip the schema so its id, persons , orgs, .... 

annotations.write.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='temp_entities', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})

# # 

# results = annotations.groupby('id').pivot('entity_type').select(collect_list('finished_ner_chunk'))

# annotations.write.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='email_entities', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})

