#from data_Pipeline import data_ingestion, data_pre_processing, spliting_data, tranin_and_testing
import sys
sys.path.append("/home/von-ray/Desktop/airflow/lib/pythonX.X/site-packages")
import os
os.environ['PYSPARK_PYTHON'] = '/home/von-ray/Desktop/airflow/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/von-ray/Desktop/airflow/bin/python'
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import when
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# data ingestion as pyspark dataframe
def data_ingestion():
    # create Spark session
    spark = SparkSession.builder.appName("FlightsDataProcessing").getOrCreate()
    # read the .CVS file as pyspark dataframe
    flights_df= spark.read.csv("/home/von-ray/code spark/Sujet d'Exam/flights.csv",header=True, inferSchema=True)
    return flights_df

# data pre-processing

def data_pre_processing(flights_df, **kwargs):
    # Drop lines with missing values
    flights_df = flights_df.dropna()

    # Supprimer les doublons du DataFrame
    flights_df = flights_df.dropDuplicates()

    # Create a binary col 'is_delayed' for the target prediction
    flights_df = flights_df.withColumn('is_delayed', when(col('DepDelay') > 0, 1).otherwise(0))

    selected_features = ['DayofMonth', 'DayOfWeek', 'Carrier', 'OriginAirportID', 'DestAirportID', 'DepDelay']
    flights_df = flights_df.select(selected_features + ['is_delayed'])
    # fill missing values by 0 for  'DepDelay'
    flights_df = flights_df.na.fill(0, subset=['DepDelay'])  

    return flights_df

def spliting_data(flights_df, **kwargs):
    #data ramdonly splited into 80% training and 20% testing
    train_data, test_data = flights_df.randomSplit([0.8, 0.2], seed=42)

    return train_data, test_data

def train_and_testing(train_data, test_data, **kwargs):

    # convert the col 'Carrier' that hold string data into numerical data under new col 'CarrierIndexTemp'
    indexer = StringIndexer(inputCol='Carrier', outputCol='CarrierIndexTmp')
    # Creation of assembler to collect the features as input to give output as linear function equation
    assembler = VectorAssembler(inputCols=['DayofMonth', 'DayOfWeek', 'CarrierIndexTmp', 'OriginAirportID', 'DestAirportID', 'DepDelay'],
                                outputCol='features')
    # Creation of a linear logistic model
    lr = LogisticRegression(featuresCol='features', labelCol='is_delayed')

    # Creation of a pipeline
    pipeline = Pipeline(stages=[indexer, assembler, lr])

    # training model on the 80% training data
    model = pipeline.fit(train_data)

    # Prediction on testing data
    predictions = model.transform(test_data)

    # evaluation of the model
    evaluator = BinaryClassificationEvaluator(labelCol='is_delayed', metricName='areaUnderROC')
    auc = evaluator.evaluate(predictions)

    # print model performance
    print(f"Area Under ROC: {auc}")

    # print prediction on it's real values
    predictions.select('is_delayed', 'prediction', 'probability').show(10)



"""
with DAG(dag_id= "FlightsDataProcessing", start_date=datetime(2023,11,14), schedule_interval = "@hourly", catchup=False) as dag:
    load_data= PythonOperator(task_id= "Data_Ingestion", python_callable=data_ingestion, provide_context=True)
    process_data= PythonOperator(task_id= "Data_Pre_Processing", python_callable=data_pre_processing, provide_context=True)
    split_data= PythonOperator(task_id= "Spliting_Data", python_callable=spliting_data, provide_context=True)
    train_test_data= PythonOperator(task_id= "Tranin_And_Testing", python_callable=tranin_and_testing, provide_context=True)

load_data >> process_data >> split_data >> train_test_data
"""

# Définir les paramètres du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialiser le DAG
dag = DAG(
    'FlightsDataProcessing',
    default_args=default_args,
    description='Orchestration du pipeline de données avec PySpark et Apache Airflow',
    schedule_interval= "@hourly",
)

# Tâche pour l'ingestion des données
data_ingestion_task = PythonOperator(
    task_id='data_ingestion',
    python_callable=data_ingestion,
    provide_context=True,
    dag=dag,
)


# Tâche pour le prétraitement des données
data_pre_processing_task = PythonOperator(
    task_id='data_pre_processing',
    python_callable=data_pre_processing,
    provide_context=True,
    dag=dag,
)

# Tâche pour la division des données
spliting_data_task = PythonOperator(
    task_id='spliting_data',
    python_callable=spliting_data,
    provide_context=True,
    dag=dag,
)

# Tâche pour l'entraînement et l'évaluation du modèle
train_and_test_task = PythonOperator(
    task_id='train_and_test',
    python_callable=train_and_testing,
    provide_context=True,
    dag=dag,
)

# Définir les dépendances entre les tâches
data_ingestion_task >> data_pre_processing_task >> spliting_data_task >> train_and_test_task