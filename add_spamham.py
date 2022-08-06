'''
pyspark --master local[6] --driver-memory 12g --jars /home/computron/jars/postgresql-42.4.0.jar --packages com.johnsnowlabs.nlp:spark-nlp_2.12:4.0.2
'''

import sparknlp
from sparknlp.annotator import ClassifierDLModel, UniversalSentenceEncoder , DocumentAssembler
from sparknlp.pretrained import PretrainedPipeline

from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.sql.functions import substring, col 

sparknlp.start() 


#assemble model! 
document_assembler = DocumentAssembler()\
.setInputCol("text")\
.setOutputCol("document")
use = UniversalSentenceEncoder.pretrained('tfhub_use', lang="en") \
.setInputCols(["document"])\
.setOutputCol("sentence_embeddings")
document_classifier = ClassifierDLModel.pretrained('classifierdl_use_spam', 'en') \
.setInputCols(["document", "sentence_embeddings"]) \
.setOutputCol("class")
pipeline = PipelineModel(stages=[document_assembler, use, document_classifier])

#get data! 

pgsql = sqlContext.read.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='emails', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})
pgsql.createOrReplaceTempView('emails')

df = sql('select id, substring(body, 0 , 4096) as text  from emails').repartition(200)

annotations = pipeline.transform(df).select('id', (col('class.result')[0] == 'spam' ).alias('is_spam')) 

annotations.write.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='email_isspam', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})

# annotations.createOrReplaceTempView('spam')

# sql('select  sum (cast ( is_spam as int) ) / count(*) as ratio  from spam ').show() 


