# ScalableDataScience
######################################################################## DATA PROCESSING ###############################################################################################################

#Q2 Complete the following data preprocessing.
#(a) Filter the Taste Profile dataset to remove the songs which were mismatched. You should
#read the documentation and blog post mentioned above carefully.

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Processing Q2 (a)
#By left anti join, song_id of the mismatched songs was removed and data in triplet was used to find the correct song_id.	

mismatches_schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("song_artist", StringType(), True),
    StructField("song_title", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_artist", StringType(), True),
    StructField("track_title", StringType(), True)
])

with open("/users/home/aka33/sid_matches_manually_accepted.txt", "r") as f:
    lines = f.readlines()
    sid_matches_manually_accepted = []
    for line in lines:
        if line.startswith("< ERROR: "):
            a = line[10:28]
            b = line[29:47]
            c, d = line[49:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_matches_manually_accepted.append((a, e, f, b, g, h))

matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted, 8), schema=mismatches_schema)
matches_manually_accepted.cache()
matches_manually_accepted.show(10, 40)

print(matches_manually_accepted.count())  # 488

with open("/users/home/aka33/sid_mismatches.txt", "r") as f:
    lines = f.readlines()
    sid_mismatches = []
    for line in lines:
        if line.startswith("ERROR: "):
            a = line[8:26]
            b = line[27:45]
            c, d = line[47:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_mismatches.append((a, e, f, b, g, h))

mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches, 64), schema=mismatches_schema)
mismatches.cache()
mismatches.show(10, 40)

print(mismatches.count())  # 19094

triplets_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("plays", IntegerType(), True)
])
triplets = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(triplets_schema)
    .load("hmatrix_methods_of_moments_dataframes:///data/msd/tasteprofile/triplets.tsv/")
    .cache()
)
triplets.cache()
triplets.show(10, 50)

mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")
triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

print(triplets.count())                 # 48373586
print(triplets_not_mismatched.count())  # 45795111

#(b) Load the audio feature attribute names and types from the audio/attributes directory
#and use them to define schemas for the audio features themselves. Note that the attribute
#files and feature datasets share the same prefix and that the attribute types are named
#consistently. Think about how you can automate the creation of StructType by mapping
#attribute types to pyspark.sql.types object

#Attribute names and types were extracted from the audio features and schema in audio for the features was found. The schema is then mapped with the feature file. 
audio_attribute_type_mapping = {
  "NUMERIC": DoubleType(),
  "real": DoubleType(),
  "string": StringType(),
  "STRING": StringType()
}

audio_dataset_names = [
  "msd-jmir-area-of-methods_of_momentsents-all-v1.0",
  "msd-jmir-lpc-all-v1.0",
  "msd-jmir-methods-of-methods_of_momentsents-all-v1.0",
  "msd-jmir-mfcc-all-v1.0",
  "msd-jmir-spectral-all-all-v1.0",
  "msd-jmir-spectral-derivatives-all-all-v1.0",
  "msd-marsyas-timbral-v1.0",
  "msd-mvd-v1.0",
  "msd-rh-v1.0",
  "msd-rp-v1.0",
  "msd-ssd-v1.0",
  "msd-trh-v1.0",
  "msd-tssd-v1.0"
]

audio_dataset_schemas = {}
for audio_dataset_name in audio_dataset_names:
  print(audio_dataset_name)

  audio_dataset_path = ("/users/home/aka33/{audio_dataset_name}.attributes.csv")
  with open(audio_dataset_path, "r") as f:
    rows = [line.strip().split(",") for line in f.readlines()]

#The dictionary is further created so that data could be used for future use.

  audio_dataset_schemas[audio_dataset_name] = StructType([
    StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows
  ])
    
  s = str(audio_dataset_schemas[audio_dataset_name])
  print(s[0:50] + " ... " + s[-50:])
  
 

######################################################################## AUDIO SIMILARITY ###############################################################################################################

#Q1 There are multiple audio feature datasets, with different levels of detail. Pick one of the small
#datasets to use for the following.
#(a) The audio features are continuous values, obtained using methods such as digital signal
#processing and psycho-acoustic modeling.
#Produce descriptive statistics for each feature column in the dataset you picked. Are any
#features strongly correlated?

from pyspark.mllib.stat import Statistics
import numpy as np
import pandas as pd
import re

print( matrix_methods_of_moments_dataframe.rdd.map(lambda row: row[0:])_file)
#  Used audio file = "msd-jmir-methods-of-methods_of_momentsents-all-v1.0"
audio_file_schema_third_audio = audio_attribute_type_mapping(files[2])
methods_of_moments = (
	spark.read.format("com.databricks.spark.csv")
    .option("inferSchema", "false")
    .option("quote", "\'")
    .schema(audio_file_schema_third_audio)
    .load( matrix_methods_of_moments_dataframe.rdd.map(lambda row: row[0:])_file)
)
descriptive_statistics_methods_of_moments = methods_of_moments.describe()
descriptive_statistics_methods_of_moments.head()

Colums_in_methods_of_moments = methods_of_moments.toPandas()  

Colums_in_methods_of_moments = Colums_in_methods_of_moments.rename(columns = {'StandardDeviation1','Average_1','StandardDeviation2','Average_2','StandardDeviation3','Average_3','StandardDeviation4','Average_4','StandardDeviation5', 'Average_5'}
)

matrix_methods_of_moments = pd.scatter_matrix(Colums_in_methods_of_moments.loc[:,'StandardDeviation1':'Average_5'], alpha=0.2, figsize=(12, 12), diagonal = 'kde');
Rows_matrix = len(Colums_in_methods_of_moments.columns[0:-3])

for i in range(Rows_matrix):
    label_head = matrix_methods_of_moments[Rows_matrix-1, i]
    label_head.xaxis.label.set_rotation(45)
    label_headhead.set_xticks(())

descriptive_statistics_methods_of_moments.toPandas().transpose()

matrix_methods_of_moments_dataframe = (
	methods_of_moments
	.drop("MSD_TRACKID")
)

correlation_matrix_methods_of_moments = Statistics.corr( matrix_methods_of_moments_dataframe.rdd.map(lambda row: row[0:]), method="pearson")
correlation_matrix_methods_of_moments_dataframe = pd.DataFrame(correlation_matrix_methods_of_moments)
correlation_matrix_methods_of_moments_dataframe.index, correlation_matrix_methods_of_moments_dataframe.columns = matrix_methods_of_moments_dataframe.columns, matrix_methods_of_moments_dataframe.columns

outlier_high_variance = np.where(correlation_matrix_methods_of_moments>0.7)
for a,b in zip(*outlier_high_variance):
  if a!=b and a<b]:
   outlier_high_variance_matrix=[(correlation_matrix_methods_of_moments_dataframe.columns[a], correlation_matrix_methods_of_moments_dataframe.columns[b])  

matrix_methods_of_moments_dataframe_pandas = pd.DataFrame(np.array(outlier_high_variance_matrix))

def correlation_value(matrix_methods_of_moments_dataframe):
	matrix_methods_of_moments_dataframe1 = []
	for i in range(len(matrix_methods_of_moments_dataframe):
		matrix_methods_of_moments_dataframe1.append(correlation_matrix_methods_of_moments_dataframe[matrix_methods_of_moments_dataframe[0][i]][matrix_methods_of_moments_dataframe[1][i]]) 
	return matrix_methods_of_moments_dataframe1

matrix_methods_of_moments_dataframe_values = pd.DataFrame(correlation_value(matrix_methods_of_moments_dataframe_pandas))

result_high_correlation_values = pd.concat([matrix_methods_of_moments_dataframe_pandas,matrix_methods_of_moments_dataframe_values], axis = 1)

new_schema_after_correlation = StructType([StructField('Variable_1',StringType(),
					   StructField('Variable_2',StringType(),
					   StructField('Value',DoubleType()
)

matrix_methods_of_moments_dataframe_high_correlation1 = sqlContext.createDataFrame(result_high_correlation_values, new_schema_after_correlation)

matrix_methods_of_moments_dataframe_high_correlation1.head()

#(b) Load the MSD All Music Genre Dataset (MAGD).
#Visualize the distribution of genres for the songs that were matched.

MAGD_dataset = (
    spark.load("hmatrix_methods_of_moments_dataframes:///data/msd/genre/msd-MAGD-genreAssignment.tsv")
)

#For doing visualisation, we look to the columns track_id and genre. 

MAGD_dataset = MAGD_dataset.select(
    F.trim(F.substring(F.col('value'), 1, 18)).alias("track_id"),
    F.trim(F.substring(F.col('value'), 20, 60)).alias("genre")
)
track_id.rename('track')
genre.rename('genre_of_track')

genre = (
     MAGD_dataset
     .groupby("genre")
     .agg({"track_id":"count"})
)

genre.count()

#Since both the variables are qualitative, we used a bar plot between track_id ncount and grouped it by genre.

genre_count_pandas = genre.toPandas()

genre_count_pandas.plot(kind = 'bar', x= 'genre', y='count(track)')

def bar_plot():
    plt.xlabel('Genre', fontsize=20)
    plt.ylabel('No of Songs', fontsize=20)
    plt.title('Distribution of songs that were matched')
    plt.savefig("barplot_final", bbox_inches='tight')

if __name__ == "__main__":
    bar_plot()

#(c) Merge the genres dataset and the audio features dataset so that every song has a label.

#The datasets: - genre and audio feature are merged via column track_id. 
genre_merge = (
	methods_of_moment
	.join(MAGD_dataset, methods_of_moment.MSD_TRACKID == MAGD_dataset.track_id)
	.drop(MAGD_dataset.track_id)
)

genre_merge.filter(F.col('genre_of_track').isNull()).count()

##Q2 First you will develop a binary classification model.
#(b) Convert the genre column into a column representing if the song is ”Rap” or some other
#genre as a binary label.
#What is the class balance of the binary label?

#Rap column is filtered with column as the binary preference. 1 value is given to the Rap track and 0 value is given to Non-rap track.

genre_merge_binary = genre_merge.withColumn("Rap", F.when(F.col("genre_of_track") == 'Rap', 1)
           .otherwise(0)
)

Numerator = genre_merge_binary.filter(F.col("Rap") == 1)
class_balance_N = Numerator.count()

Denominator = genre_merge_binary.filter(F.col("Rap") == 0)
class_balance_D = Denominator.count()

class_balance = class_balance_N/class_balance_D
print(class_balance)

#The class balance of the binary label is coming to be 0.05228.

#(c) Split the dataset into training and test sets. Note that you may need to take class balance
#into account using a sampling method such as stratification, subsampling, or oversampling.
#Justify your choice of sampling method.

genre_merge_binary_second = genre_merge_binary.filter(F.col("Standard_deviation_first") != 0)

Difference_between_genres = genre_merge_binary.count()-genre_merge_binary_second.count()
print(Difference_between_genres)

genre_merge_binary_second = genre_merge_binary_second.withColumnRenamed("Rap", "label")

genre_merge_binary_non_numeric = genre_merge_binary_second.drop("MSD_TRACKID", "MAGD_dataset.track_id", "genre_of_track")

from pyspark.ml.feature import VectorAssembler
assembler = (VectorAssembler()
            .setInputCols(genre_merge_binary_non.columns[:-1])
            .setOutputCol('features')
            )
assembler.transform(genre_merge_binary_non_numeric).count()

dataset_split = assembler.transform(genre_merge_binary_non_numeric).randomSplit([0.7, 0.3], seed = 100)

dataset_split[0].groupBy("label").count().orderBy("label").show()

dataset_split[1].groupBy("label").count().orderBy("label").show()


train = dataset_split[0].sampleBy("label", fractions={0:0.11, 1:1})
train.groupBy("label").count().orderBy("label").show()

test = dataset_split[1]

test.groupBy("label").count().orderBy("label").show()

#(d) Train each of the three classification algorithms that you chose in part (a).
#(e) Use the test set to compute the compute a range of peRformance metrics for each model,
#such as precision, accuracy, and recall.
#(f) Discuss the relative peRformance of each model and of the classification algorithms overall,
#taking into account the peRformance metrics that you computed in part (e).
#How does the class balance of the binary label affect the peRformance of the algorithms?

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


def binary_evaluator(matrix_methods_of_moments_dataframe):
    total = matrix_methods_of_moments_dataframe.count()
    TP = matrix_methods_of_moments_dataframe[(matrix_methods_of_moments_dataframe.label == 1) & (matrix_methods_of_moments_dataframe.prediction == 1)].count()
    TN = matrix_methods_of_moments_dataframe[(matrix_methods_of_moments_dataframe.label == 0) & (matrix_methods_of_moments_dataframe.prediction == 0)].count()
    FP = matrix_methods_of_moments_dataframe[(matrix_methods_of_moments_dataframe.label == 0) & (matrix_methods_of_moments_dataframe.prediction == 1)].count()
    FN = matrix_methods_of_moments_dataframe[(matrix_methods_of_moments_dataframe.label == 1) & (matrix_methods_of_moments_dataframe.prediction == 0)].count()
    recall_val = float(TP)/(TP + FN)
    precision_val = float(TP) / (TP + FP)
    f1score = 1/((1/recall_val+1/precision_val)/2)
    binary_evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC")
    auroc = binary_evaluator.evaluate(matrix_methods_of_moments_dataframe)
    print('auroc: {}'.format(auroc))
    print('F1_Score: {}'.format(f1score))
    print('True Positive: {}'.format(TP))
    print('True Negative: {}'.format(TN))
    print('False Positive: {}'.format(FP))
    print('False Negative: {}'.format(FN))
    print('precision: {}'.format(TP / (TP + FP)))
    print('recall: {}'.format(TP / (TP + FN)))
    print('accuracy: {}'.format((TP + TN) / total))
    return()

#Training models
Rf = RandomForestClassifier(maxDepth=5,  maxBins=32, numTrees=20, seed=100)
Rf_fitting = Rf.fit(train)

Dt = DecisionTreeClassifier(maxDepth=5, maxBins=32, seed=100)
Dt_fitting = Dt.fit(train)

gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=100)
gbtt_fitting = gbt.fit(train)

#Testing models

rf_testing = Rf_fitting.transform(test)
binary_evaluator(rf_testing)

Dt_prediction = Dt_fitting.transform(test)
binary_evaluator(Dt_prediction)

gbt_prediction = gbtt_fitting.transform(test)
binary_evaluator(gbt_prediction)

#Q3 Now you will tune the hyperparameters of your binary classification model.
#(a) Look up the hyperparameters for each of your classification algorithms. Try to understand
#how these hyperparameters affect the performance of each model and if the values you
#used in Q2 part (d) were sensible
#(b) Use cross-validation to tune some of the hyperparameters of your best performing binary
#classification model.
#How has this changed your performance metrics?

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

#Training models
evaluator_2 = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC")

Rf_CV_parameters = (ParamGridBuilder()
               .addGrid(Rf.numTrees, [15,30,45])
               .addGrid(Rf.maxDepth, [8,9,10,11,12,13,14,15,16,17,18])
               .addGrid(Rf.maxBins, [32, 38, 44, 50])
               .build())

Rf_CV = CrossValidator(estimator=Rf,\
                       estimatorParamMaps=Rf_CV_parameters,\
                       evaluator=evaluator,\
                       numFolds=10)

Rf_CV_fitting = Rf_CV.fit(train)

Rf_CVbestModel = Rf_CV_fitting.bestModel
print('NumTrees: ', Rf_CVbestModel._java_obj.getNumTrees())
print('MaxDepth: ', Rf_CVbestModel._java_obj.getMaxDepth())
print('MaxBins: ', Rf_CVbestModel._java_obj.getMaxBins())

gbt_CV_parameters = (ParamGridBuilder()
                .addGrid(gbt.maxDepth, [5,6,8,10])
                .addGrid(gbt.maxBins, [20,30,50])
                .addGrid(gbt.maxIter, [15,20])
                .build())

evaluator_3 = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC")

gbt = CrossValidator(estimator=gbt, estimatorParamMaps=gbt_CV_parameters, evaluator=evaluator_3, numFolds=10)

gbt_cv = CrossValidator(estimator=gbt,\
                       estimatorParamMaps=gbt_CV_parameters,\
                       evaluator_3=evaluator_3,\
                       numFolds=10)


gbt_CV_fitting = gbt_cv.fit(train)

cvGBTBestModel = gbt_CV_fitting.bestModel
print('NumTrees: ', Rf_CVbestModel._java_obj.getNumTrees())
print('MaxDepth: ', Rf_CVbestModel._java_obj.getMaxDepth())
print('MaxBins: ', Rf_CVbestModel._java_obj.getMaxBins())


Dt_paramGrid = (ParamGridBuilder()
               .addGrid(Dt.maxDepth, [6, 7, 8, 11, 12])
               .addGrid(Dt.maxBins, [2, 16, 32, 48])
               .build())

evaluator_4 = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC")

Dtm = CrossValidator(estimator=Dt, estimatorParamMaps=Dt_paramGrid, evaluator=evaluator_4, numFolds=10)
 
dtm_CV = Dtm.fit(train)

cvDtbestModel = dtm_CV.bestModel

print('maxDept: ', cvDtbestModel._java_obj.getMaxDepth())
print('maxBins: ', cvDtbestModel._java_obj.getMaxBins())

#Testing models

Rf_CV_testing = Rf_CVbestModel.transform(test)
binary_evaluator(Rf_CV_testing)

gbt_cv_testing = gbt_CV_fitting.transform(test)
binary_evaluator(gbt_cv_testing)

predCVDt = dtm_CV.transform(test)
binary_evaluator(predCVDt)

#Before cross validation, gradient boosting model was performing best. After cross validation, Random forest performed the best in comparison to gradient boosting and #decision tree.

#Q4 Next you will extend your work above to predict across all genres .
#(a) Choose one of your algorithms from Q2 that is capable of multiclass classification and
#performed well for binary classification as well.
#(b) Convert the genre column into an integer index that encodes each genre consistently. Find
#a way to do this that requires the least amount of work by hand.
#(c) Split your dataset into training and test sets, train the classification algorithm that you chose
#in part (b), and compute a range of performance metrics that are relevant to multiclass
#classification. Make sure you take into account the class balance in your comments.
#How has the performance of your model been affected by the inclusion of multiple genres?

from pyspark.ml.classification import RandomForestClassifier, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

genre_Moments_data = genre_mom.drop('MSD_TRACKID')

from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="genre", outputCol="label").fit(genre_Moments_data)
genre_Moments_data_2 = indexer.transform(genre_Moments_data)

genre_Moments_data_3 = genre_Moments_data_2.drop('genre')

from pyspark.ml.feature import VectorAssembler
assembler = (VectorAssembler()
            .setInputCols(genre_Moments_data_3.columns[:-1])
            .setOutputCol('features')
            )
genre_assembler = assembler.transform(genre_Moments_data_3)

fgenre_assembler_trail = genre_assembler.toPandas()

cp_features = features_3b_trail['label'].unique()
class_proportion_list ={}
for i in cp_features:
         class_proportion_list[i] = 0.7

train_multiple = genre_assembler.sampleBy("label", fractions=class_proportion_list, seed = 100)
test_multiple = features_3b.subtract(train_multiple)


def evaluator_multiclass(predictor):
  total_number = predictor.count()

  evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="f1")
  f1 = evaluator.evaluate(predictor)

  evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="weighted_precision")
  weighted_precision = evaluator.evaluate(predictor)

  evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="weightedRecall")
  weightedRecall = evaluator.evaluate(predictor)

  evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="accuracy")
  accuracy = evaluator.evaluate(predictor)
  testerror = str(1 - accuracy)

  print('total_number: {}'.format(total_number))
  print('f1: {}'.format(f1))
  print('weighted_precision: {}'.format(weighted_precision))
  print('weightedRecall: {}'.format(weightedRecall))
  print('Accuracy: {}'.format(accuracy))
  print('testerror: {}'.format(testerror))
  return()

rf = RandomForestClassifier(maxDepth=5,  maxBins=32, numTrees=20, seed=100)
rf_fitting = rf.fit(train_multiple)
rf_testing = rf_fitting.transform(test_multiple)

evaluator_multiclass(rf_testing)

top_model = OneVsRest(classifier=rf)
top_model_model = top_model.fit(train_multiple)


evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="f1")

rf_parameters = (ParamGridBuilder()
             .addGrid(rf.maxDepth, [7, 8, 11, 12])
             .addGrid(rf.maxBins, [32, 40, 48, 54])
             .addGrid(rf.numTrees, [20, 50, 100])
             .build())

cv_rf = CrossValidator(estimator=rf, estimatorParamMaps=rf_parameters, evaluator=evaluator, numFolds=10)
 
cvModel_rf = cv_rf.fit(train_multiple)
 
best_rf_fitting = cvModel_rf.bestModel

print('maxDepth: ', best_rf_fitting._java_obj.getMaxDepth())
print('maxBins: ', best_rf_fitting._java_obj.getMaxBins())
print('numTrees: ', best_rf_fitting._java_obj.getNumTrees())


#Testing

top_model_testing = top_model_model.transform(test_multiple)

evaluator_multiclass(top_model_testing)

prediction_rf_cv = best_rf_fitting.transform(test_multiple)

######################################################################## SONG user_song_recommendATIONS ###############################################################################################################

#Q1 First it will be helpful to know more about the properties of the dataset before you being training
#the collaborative filtering model.
#(a) How many unique songs are there in the dataset? How many unique users?
from pyspark.sql.functions import countDistinct


unique_songs = triplets_not_mismatched.agg(countDistinct("song_id").alias('unique_songs'))
unique_songs.show()

unique_users = triplets_not_mismatched.agg(countDistinct("user_id").alias('user_id'))

#(b) How many different songs has the most active user played?
#What is this as a percentage of the total number of unique songs in the dataset?

triplets_not_mismatched.registerTempTable('song_user_activity')
most_active_users = spark.sql("""
 SELECT user_id, sum(play_count) AS playcounts
 FROM song_user_activity
 GROUP BY 1
 ORDER BY 1 DESC
"""
)
most_active_users.show(10)


different_songs_single_user = spark.sql("""
SELECT 
a.user_id, 
COUNT(distinct song_id) AS unique_songs
FROM 
song_user_activity a
JOIN
 (
  SELECT 
  user_id, 
  ROW_NUMBER() OVER (ORDER BY sum(play_count) DESC) rank
  FROM 
   song_user_activity
  GROUP BY 1
 ) b 
 ON a.user_id = b.user_id AND b.rank = 1
GROUP BY 1
""")

different_songs_single_user.show(10)

percentage_of_total = (different_songs_single_user.count()/unique_songs.count())*100
print(percentage_of_total)

#(c) Visualize the distribution of song popularity and the distribution of user activity.
#What is the shape of these distributions?

song_popularity = (
    triplets_not_mismatched
    .select(['song_id','play_count'])
    .groupBy('song_id')
    .agg({'play_count': 'sum'})
    .orderBy('sum(play_count))
    .select(
        F.col('song_id'),
        F.col('sum(play_count)').alias('total_counts_play')
    )
)

distribution_of_user_activity = (
    triplets_not_mismatched
    .select(['user_id','song_id'])
    .groupBy('user_id')
    .agg({'song_id': 'count'})
    .orderBy('count(song_id)')
    .select(
        F.col('user_id'),
        F.col('count(song_id)').alias('total_counts_play')
    )
)

user_activity_pandas = distribution_of_user_activity.toPandas()

user_activity_pandas.hist(column='total_counts_play', bins=50)
plt.xlabel('play_count', fontsize=12)
plt.ylabel('songs', fontsize=12)
plt.title('User Activity Distribution')
plt.tight_layout()
plt.savefig(f"DATA420distribution_of_user_activity.png", bbox_inches="tight")


popular_songs_pandas = song_popularity.toPandas()

popular_songs_pandas.hist(column='total_counts_play', bins=50)
plt.xlabel('play_count', fontsize=12)
plt.ylabel('Songs', fontsize=12)
plt.title('Distribution of Song Popularity')
plt.tight_layout()
plt.savefig(f"DATA420song_popularity_distribution.png", bbox_inches="tight")

#(d) Collaborative filtering determines similar users and songs based on their combined play
#history. Songs which have been played only a few times and users who have only listened
#to a few songs will not contribute much information to the overall dataset and are unlikely
#to be user_song_recommended.
#Create a clean dataset of user-song plays by removing songs which have been played less
#than N times and users who have listened to fewer than M songs in total. Choose sensible
#values for N and M and justify your choices, taking into account (a) and (b).

distribution_of_user_activity.registerTempTable('activity_user_tbl')

threshold_limit_of_user = spark.sql("""
    SELECT PERCENTILE(total_counts_play, 0.25) 
    FROM activity_user_tbl
    """
    )

most_active_user = distribution_of_user_activity.filter(distribution_of_user_activity["total_counts_play"] > 15)
song_popularity.registerTempTable('popular_songs_tbl')


threshold_limit_of_popular_song = spark.sql("""
    SELECT PERCENTILE(total_counts_play, 0.25) 
    FROM popular_songs_tbl
    """
    )

Highest_of_popular_song = popular_songs.filter(popular_songs["total_counts_play"] > 8)
Highest_of_popular_song.show(10)

most_played_song = (
    triplets_not_mismatched
    .select(['user_id', 'song_id', 'play_count'])
    .join(
        Highest_of_popular_song
        .select(
            'song_id'),
        on='song_id',
        how='inner'
    )
    .join(
        most_active_user
        .select(
            'user_id'),
        on='user_id',
        how='inner'
        )    
)

print(most_played_song.count()/ triplets_not_mismatched.count())

from pyspark.sql.functions import isnan, when, count, col

most_played_song.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in most_played_song.columns]).show()

#(e) Split the user-song plays into training and test sets. The test set should contain at least
#25% of the plays in total.
#Note that due to the nature of the collaborative filtering model, you must ensure that every
#user in the test set has some user-song plays in the training set as well. Explain why this is
#required and how you have done this while keeping the selection as random as possible.

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.mllib.stat import Statistics
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.feature import StringIndexer

indexer_user_id = StringIndexer(inputCol="user_id", outputCol="user")
indexer_song_id = StringIndexer(inputCol="song_id", outputCol="item")
indexed_1 = indexer_user_id.fit(most_played_song).transform(most_played_song)
complete_data = indexer_song_id.fit(indexed_1).transform(indexed_1)


unique_data = dataset_als.dropDuplicates(["user_label_int"]).cache()

unique_data_temp = dataset_als.subtract(unique_data)

(train_data_temp, test_data) = unique_data_temp.randomSplit([0.7, 0.3], seed=100)

als_trainingdata = unique_data.union(train_data_temp)
test_data.count()

#Q2 Next you will train the collaborative filtering model.
#(a) Use the spark.ml library to train an implicit matrix factorization model using Alternating
#Least Squares (ALS).

from pyspark.ml.user_song_recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

als = ALS(implicitPrefs=True, userCol="user_label_int", itemCol="song_label_int", ratingCol="play_count", coldStartStrategy="drop")
          
als_fitting = als.fit(als_trainingdata)
als_fitting.transform(test_data).show()

#(b) Select a few of the users from the test set by hand and use the model to generate some
#user_song_recommendations. Compare these user_song_recommendations to the songs the user has actually
#played. Comment on the effectiveness of the collaborative filtering model.
#(c) Use the test set of user-song plays and user_song_recommendations from the collaborative filtering
#model to compute the following metrics
#• Precision @ 5
#• NDCG @ 10
#• Mean Average Precision (MAP)
#Look up these metrics and explain why they are useful in evaluating the collaborate filtering
#model. Explore the limitations of these metrics in evaluating a user_song_recommendation system in
#general. 

some_users = testdata.select(als.getUserCol()).filter(testdata["user_label_int"].rlike("180[0-5]")).distinct()

some_users_user_song_recommend = als_fitting.user_song_recommendForUserSubset(some_users, 5)

def user_song_recommend(user_song_recommendations):
    n = []
    for n, rating in user_song_recommendations:
        n.append(n)
    return n

udf_user_song_recommend = F.udf(lambda user_song_recommendations: user_song_recommend(user_song_recommendations), ArrayType(IntegerType()))

temp = (
     user_song_recommend.withColumn('user_song_recommends', udf_user_song_recommend(F.col('user_song_recommendations')))
    .select(['user_label_int', 'user_song_recommends'])
    )

testdata = testdata.groupBy("user_label_int").agg(F.collect_list("song_label_int"))

metrics_testdata = temp.join(testdata, on="user_label_int", how="inner")
metrics_testdata = metrics_testdata.drop("user_label_int")
rdd_testing = metrics_testdata.rdd

from pyspark.mllib.evaluation import RankingMetrics

computing = RankingMetrics(rdd_testing)

computing.precisionAt(5)
computing.ndcgAt(10)
computing.meanAveragePrecision()

