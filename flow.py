'''
pyspark --master local[6] --driver-memory 8g --jars /home/computron/postgresql-42.4.0.jar
'''

from flanker import mime 
from flanker.addresslib import address  
from dateutil import parser as dateparser 


from pyspark.sql.functions import input_file_name, regexp_replace, trim, udf 
from pyspark.sql.types import StructType, StringType, ArrayType , TimestampType


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
raw = spark.read.text('/home/computron/dataflows/enron/maildir/**/*', wholetext=True)
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

df.write.jdbc(url='jdbc:postgresql://localhost:5432/computron', table='emails', mode='append', properties={'driver':'org.postgresql.Driver', 'user': 'computron','password': 'password'})


'''
CREATE TABLE IF NOT EXISTS emails ( 
  id serial PRIMARY KEY,
  "date" TIMESTAMP, 
  "from" TEXT NOT NULL,
  "to" TEXT[],
  "cc" TEXT[],
  bcc TEXT[],
  subject TEXT, 
  body TEXT,
  filepath TEXT,
  "mime-version"  TEXT ,
  "x-filename" TEXT
);
CREATE TABLE IF NOT EXISTS email_language ( 
  id integer references emails(id),
  language text
);

create index idx_emails_from on emails("from");
create index idx_emaillang_id on email_language(id);


'''