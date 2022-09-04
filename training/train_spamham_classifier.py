'''

spark-submit --master local[6] --driver-memory 32g --packages org.postgresql:postgresql:42.4.0   --packages com.johnsnowlabs.nlp:spark-nlp-gpu_2.12:4.1.0 train_spamham_classifier.py

'''

import sparknlp
from sparknlp.annotator import ClassifierDLModel, ClassifierDLApproach, UniversalSentenceEncoder , DocumentAssembler
from sparknlp.pretrained import PretrainedPipeline

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext 

from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.sql.functions import substring, col , lit, sum 
from pyspark.ml.tuning import  TrainValidationSplit

spark = sparknlp.start(gpu=True) 

sql = SQLContext(spark.sparkContext).sql

#assemble model! 
document_assembler = DocumentAssembler()\
.setInputCol("text")\
.setOutputCol("document")
use = UniversalSentenceEncoder.pretrained('tfhub_use', lang="en") \
.setInputCols(["document"])\
.setOutputCol("sentence_embeddings")
document_classifier = ClassifierDLApproach() \
    .setInputCols("sentence_embeddings") \
    .setOutputCol("category") \
    .setLabelColumn("label") \
    .setBatchSize(64) \
    .setMaxEpochs(10) \
    .setLr(5e-3) \
    .setDropout(0.5)



pipeline = Pipeline(stages=[document_assembler, use, document_classifier])

#get data! 

ham = spark.read.text('/home/user/flowstate/training_data/spam_ham/ham/*', wholetext=True).withColumn('label', lit('ham'))
spam = spark.read.text('/home/user/flowstate/training_data/spam_ham/spam/*', wholetext=True).withColumn('label', lit('spam'))

df = ham.union(spam).withColumnRenamed('value','text').coalesce(300)
train, test = df.randomSplit([0.8, 0.2], seed=42)

model = pipeline.fit(train)

#evaluate this vs the base model 
results_newmodel = model.transform(test).select('label', col('category.result')[0].alias('prediction'))

#load the old pipeline
document_assembler = DocumentAssembler()\
.setInputCol("text")\
.setOutputCol("document")
use = UniversalSentenceEncoder.pretrained('tfhub_use', lang="en") \
.setInputCols(["document"])\
.setOutputCol("sentence_embeddings")
document_classifier = ClassifierDLModel.pretrained('classifierdl_use_spam', 'en') \
.setInputCols(["document", "sentence_embeddings"]) \
.setOutputCol("class")

pipeline_original = PipelineModel(stages=[document_assembler, use, document_classifier])

results_oldmodel = pipeline_original.transform(test).select('label', col('class.result')[0].alias('prediction')) 

#get the results! drumroll .... 

results_oldmodel.createOrReplaceTempView('old')
results_newmodel.createOrReplaceTempView('new')

print(sql('select label, sum (cast ( prediction == label as int ) ) / count(*) as ratio_old  from old  group by label ').show() )

print(sql('select label, sum (cast ( prediction == label as int ) ) / count(*) as ratio_new  from new  group by label ').show() )

# model.save('/home/user/models/hamspam-enron/')

#the results show SIGNIFICANT improvement - so deploy to add_spamham! 

