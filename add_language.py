'''
spark-submit --master local[7] --driver-memory 32g --packages org.postgresql:postgresql:42.4.0 add_language.py
'''

import cld3 
from pyspark.sql.functions import substring, col, udf 
from pyspark.sql import SparkSession 

from pyspark.sql.types import StringType

spark = (
    SparkSession
    .builder
    .appName("Add language with cld3")
    .getOrCreate())

spark.sparkContext.setLogLevel('WARN')

detect_lang = udf ( lambda text : cld3.get_language(text).language , StringType() )

df = spark.read.format("jdbc").option("url", 'jdbc:postgresql://localhost:5432/emails').option('driver','org.postgresql.Driver').option("query", "select id, substring(body, 0 , 4098) as text from emails").option("user", "flow").option("password", "password").option('fetchsize', '1000').load().repartition(240)
df = df.select('id', detect_lang('text').alias('language'))
df.write.jdbc(url='jdbc:postgresql://localhost:5432/emails', table='language', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'flow','password': 'password'})


# df.createOrReplaceTempView('results')
# sql('select language, count(*) from results group by language order by count(*) desc ').show() 
# enriched = sql( 
#     "select emails.* , annotated.language as language from emails inner join annotated on emails.id = annotated.id  "
# )
