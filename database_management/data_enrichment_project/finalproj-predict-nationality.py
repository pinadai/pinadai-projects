"""A subdag that predicts the nationality of each movie's director in the staging Film table."""
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
def _predict_nationality(**kwargs):
    project_id = kwargs["dag_run"].conf.get("project_id")
    region = kwargs["dag_run"].conf.get("region")
    stg_dataset_name = kwargs["dag_run"].conf.get("stg_dataset_name")
    stg_ai_dataset_name = kwargs["dag_run"].conf.get("stg_ai_dataset_name")

    bq_client = bigquery.Client(project=project_id, location=region)

    # create remote connection
    remote_sql = (f"""
    create or replace model remote_model_central.gemini_pro
        remote with connection `projects/composer-mahek-daisy/locations/us-central1/connections/us-central1-ai`
        options (endpoint = 'gemini-pro')""")

    bq_client.query(remote_sql).result()


    # FILM DIRECTOR NATIONALITY PREDICTIONS
    # Predicting director nationality in the Film table
    # creating the prompt
    prompt_sql = (f"""
    declare prompt_query STRING;
    set prompt_query = "For the given movie and its director, suggest a nationality based on the origin of the director's last name. Return the output as a JSON object, including only the movie's name, the director's name, and the nationality. If the nationality is indiscernable, write the nationality as 'Unknown'.";
    """)
    
    bq_client.query(prompt_sql).result()

    # generating the data
    nationality_data_sql = (f""" 
    create or replace table {stg_ai_dataset_name}.nationality_predictions_raw_full as
    select *
    from ML.generate_text(
    model remote_model_central.gemini_pro,
    (
        select concat(prompt_query, to_json_string(json_object("name", name, "director", director))) as prompt
        from {stg_dataset_name}.Film
        order by name
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
    )
    """)

    bq_client.query(nationality_data_sql).result()


    # format the JSON
    json_format_sql = (f"""
    create or replace table {stg_ai_dataset_name}.nationality_predictions_formatted_full as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\n', '')) as ml_generate_text_llm_result
    from {stg_ai_dataset_name}.nationality_predictions_raw_full
    """)

    bq_client.query(json_format_sql).result()


    # update data in staging table
    insert_nationalities_sql = (f"""
    update {stg_dataset_name}.Film f
    set d_nationality = json_value(p.ml_generate_text_llm_result, '$.nationality')
    from {stg_ai_dataset_name}.nationality_predictions_formatted_full p
    where f.name = json_value(p.ml_generate_text_llm_result, '$.name')
    """)

    bq_client.query(insert_nationalities_sql).result()

    # update the data source
    data_source_sql = (f"""
    update {stg_dataset_name}.Film
    set data_source = 'kaggle_ai' where 
    d_nationality is not null
    """)

    bq_client.query(data_source_sql).result()


with models.DAG(
    "finalproj-predict-nationality",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    predict_nationality = _predict_nationality()

    # task dependencies
    predict_nationality