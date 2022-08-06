'''
export TF_ENABLE_ONEDNN_OPTS=0
pyspark --master local[6] --driver-memory 12g --jars /home/computron/postgresql-42.4.0.jar --packages com.johnsnowlabs.nlp:spark-nlp_2.12:4.0.2
'''
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

from pyspark.sql.functions import substring, col 

sparknlp.start() 


pipeline = PretrainedPipeline("detect_language_375", disk_location='/home/computron/models/detect_language_375_xx_2.7.0_2.4_1607185980306', lang = "xx")

pgsql = sqlContext.read.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='emails', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})
pgsql.createOrReplaceTempView('emails')

df = sql('select id, substring(body, 0 , 1024) as text  from emails ').repartition(200)
annotations = pipeline.annotate(df, column='test').select('id', col('language.result')[0].alias('language') )

annotations.createOrReplaceTempView('annotated')

annotations.write.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='email_language', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})

# enriched = sql( 
#     "select emails.* , annotated.language as language from emails inner join annotated on emails.id = annotated.id  "
# )
