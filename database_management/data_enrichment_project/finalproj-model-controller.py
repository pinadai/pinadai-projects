# final project model-controller

import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

project_id = 'composer-mahek-daisy'
stg_dataset_name = 'country_stg_af'
stg_ai_dataset_name = 'country_stg_ai_af'
remote_models = 'remote_model_central'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='finalproj-model-controller',
    default_args=default_args,
    description='controller dag',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)
wait = TimeDeltaSensor(task_id="wait", delta=duration(seconds=5), dag=dag)

# create stg ai dataset
create_dataset_stg_ai_af = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_stg_ai_af',
    project_id=project_id,
    dataset_id=stg_ai_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)


# create a dataset to store the AI models
# create_remote_models = BigQueryCreateEmptyDatasetOperator(
#     task_id='create_remote_models',
#     project_id=project_id,
#     dataset_id="remote_models",
#     location=region,
#     if_exists='ignore',
#     dag=dag)

# create war probability (subdag)
create_war_probability = TriggerDagRunOperator(
    task_id="create_war_probability",
    trigger_dag_id="create-war-probability",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "stg_ai_dataset_name": stg_ai_dataset_name},
    dag=dag)

# Scenario: Use population, life expectancy, and outside knowledge of 
# country resources to predict life expectancy rates in year 2100 and 3000
# Scenario: Use population, life expectancy, and outside knowledge of 
# country resources to predict the population in year 2100 and 3000
# trigger subdag
predict_social = TriggerDagRunOperator(
    task_id="predict_social",
    trigger_dag_id="finalproj-predict-social",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "stg_ai_dataset_name": stg_ai_dataset_name},
    dag=dag)

# Scenario: Detect the livability of cities in the City staging table
detect_city_livability = TriggerDagRunOperator(
    task_id="detect_city_livability",
    trigger_dag_id="finalproj-predict-livability",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "stg_ai_dataset_name": stg_ai_dataset_name},
    dag=dag)

# Scenario: Generate a review for each movie based on the movie rank, name, and director in the Film staging table
generate_movie_review = TriggerDagRunOperator(
    task_id="generate_movie_review",
    trigger_dag_id="finalproj-generate-review",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "stg_ai_dataset_name": stg_ai_dataset_name},
    dag=dag)

# Scenario: Predict the nationality of each movie's director in the Film staging table
predict_director_nationality = TriggerDagRunOperator(
    task_id="predict_director_nationality",
    trigger_dag_id="finalproj-predict-nationality",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "stg_ai_dataset_name": stg_ai_dataset_name},
    dag=dag)

# Scenario: Input the missing country names and codes in the Master_Language staging table
countries_codes = TriggerDagRunOperator(
    task_id="countries_codes",
    trigger_dag_id="finalproj-countries-codes",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "stg_ai_dataset_name": stg_ai_dataset_name},
    dag=dag)


start >> create_dataset_stg_ai_af

create_dataset_stg_ai_af >> [create_war_probability, generate_movie_review, predict_director_nationality, detect_city_livability, predict_social, countries_codes] >> end
