'''
pyspark --master local[6] --driver-memory 8g --packages org.postgresql:postgresql:42.4.0 --packages com.johnsnowlabs.nlp:spark-nlp-gpu_2.12:4.1.0


spark-submit --master local[6] --driver-memory 8g --packages org.postgresql:postgresql:42.4.0 flow.py 
'''

from flanker import mime 
from flanker.addresslib import address  
from dateutil import parser as dateparser 

from pyspark import SparkContext 
from pyspark.sql import SparkSession

from pyspark.sql.functions import input_file_name, regexp_replace, trim, udf 
from pyspark.sql.types import StructType, StringType, ArrayType , TimestampType


spark = (
    SparkSession
    .builder
    .appName("ETL email directory into postgres")
    .getOrCreate())

spark.sparkContext.setLogLevel('WARN')

def parse_email (text : str ) -> dict : 
    #the element is the text of an email, MIME formatted 
    email = mime.from_string(text)
    element = {} 
    element['body'] = email.body 

    for key, value in email.headers.items() : 
        if  (key.startswith('X-') and key not in {'X-Filename'}  ) or key.startswith('Content-') or key.lower() == 'message-id': 
            continue 
        element[key.lower()] = value 
    return element

#setup columns for the struct
email_columns = ['date','from','to','cc','bcc','subject','body','mime-version','x-filename']

#build the struct
email_struct = StructType() 
for column in email_columns: 
    email_struct = email_struct.add(column , StringType()) 

#build custom processing via UDF's 
emailparser = udf(parse_email, email_struct)
dateformatter = udf ( lambda x : dateparser.parse(x) , TimestampType()) #.strftime('%F %T')
emailformatter = udf ( lambda x : [address.strip() for address in x.split(',') ] if x is not None else None, ArrayType(StringType()))

#read the data
raw = spark.read.text('/home/user/data/enron-maildir/**/*', wholetext=True).coalesce(240)
#add the filename 
raw = raw.withColumn("filepath", input_file_name())
#parse out email specific fields into their own fields 
df = raw.withColumn('email', emailparser(raw['value']))
df = df.select('filepath','value','email.*').withColumnRenamed('value','raw')


#format the data
df = df.select(
    dateformatter('date').alias('date'), 
    'from',
    emailformatter('to').alias('to'),
    emailformatter('cc').alias('cc'),
    emailformatter('bcc').alias('bcc'),
    'subject', 
    regexp_replace('body', '[\W]+', ' ' ).alias('body'),
    # regexp_replace('raw', '[\W]+', ' ' ).alias('raw'),
    'filepath',
    'mime-version',
    'x-filename'
)

df.write.jdbc(url='jdbc:postgresql://localhost:5432/emails', table='emails', mode='append', properties={'driver':'org.postgresql.Driver', 'user': 'flow','password': 'password'})

