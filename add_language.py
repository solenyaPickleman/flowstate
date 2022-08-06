'''
pyspark --master local[6] --driver-memory 12g --jars /home/computron/jars/postgresql-42.4.0.jar 
'''

import cld3 
from pyspark.sql.functions import substring, col, udf 
from pyspark.sql.types import StringType



detect_lang = udf ( lambda text : cld3.get_language(text).language , StringType() )

pgsql = sqlContext.read.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='emails', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})
pgsql.createOrReplaceTempView('emails')

df = sql('select id, substring(body, 0 , 4098) as text  from emails').repartition(200)

df = df.select('id', detect_lang('text').alias('language'))

df.write.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='email_language', mode='overwrite', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})


# df.createOrReplaceTempView('results')
# sql('select language, count(*) from results group by language order by count(*) desc ').show() 
# enriched = sql( 
#     "select emails.* , annotated.language as language from emails inner join annotated on emails.id = annotated.id  "
# )
