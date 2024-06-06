"""This is a subdag that detects the livability of cities."""

import airflow
from airflow import models
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
import json, datetime

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

@task(trigger_rule="all_done")
def _predict_livability(**kwargs):
    project_id = kwargs["dag_run"].conf.get("project_id")
    region = kwargs["dag_run"].conf.get("region")
    stg_dataset_name = kwargs["dag_run"].conf.get("stg_dataset_name")
    stg_ai_dataset_name = kwargs["dag_run"].conf.get("stg_ai_dataset_name")
    remote_models = kwargs["dag_run"].conf.get("remote_models")

    bq_client = bigquery.Client(project=project_id, location=region)

    # create remote connection
    remote_sql = (f"""
    create or replace model {remote_models}.gemini_pro
      remote with connection `projects/{project_id}/locations/us-central1/connections/us-central1-ai`
      options (endpoint = 'gemini-pro')""")
    
    bq_client.query(remote_sql).result()


    # CITY LIVABILITY PREDICTIONS
    # creating the prompt
    prompt_sql = (f"""
    declare prompt_query STRING default "Write a review on how livable this given city is. Some factors to consider could be walkability, safety, nightlife, excursions, etc. Return the output as json, include the city id which is attribute 'city_id' in the output as well."
    """)
    
    bq_client.query(prompt_sql).result()

    # generating the data
    livability_data_sql = (f""" 
    create or replace table {stg_ai_dataset_name}.city_predictions_full as
    select *
    from ML.generate_text(
        model {remote_models}.gemini_pro,
        (
        select concat(prompt_query, to_json_string(json_object("city_id", city_id, "name", name, "country_code", country_code,
                    "district", district, "population", population))) as prompt
        from {stg_dataset_name}.City
        order by city_id
        ),
        struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
    )
    """)

    bq_client.query(livability_data_sql).result()


    # format the JSON
    json_format_sql = (f"""
    create or replace table {stg_ai_dataset_name}.city_predictions_formatted_full as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\n', '')) as ml_generate_text_llm_result
    from {stg_ai_dataset_name}.city_predictions_full
    """)

    bq_client.query(json_format_sql).result()

    # add column to the staging table
    livability_column = (f"""
    alter table {stg_dataset_name}.City add column livability string
    """)

    # update data in staging table
    insert_livability_sql = (f"""
    update {stg_dataset_name}.City set livability =
    (select json_query(ml_generate_text_llm_result, '$.livability')
    from {stg_ai_dataset_name}.city_predictions_formatted_full
    where city_id = CAST(json_value(ml_generate_text_llm_result, '$.city_id') AS INT64))
    where 1=1
    """)

    bq_client.query(insert_livability_sql).result()

    # update the data source
    data_source_sql = (f"""
    update {stg_dataset_name}.City
    set data_source = 'bird_ai' where 
    livability is not null
    """)

    bq_client.query(data_source_sql).result()


with models.DAG(
    "finalproj-predict-livability",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    predict_livability = _predict_livability()

    # task dependencies
    predict_livability