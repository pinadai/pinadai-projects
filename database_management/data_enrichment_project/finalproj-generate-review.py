"""This is a subdag that generates a review for each movie based on the movie rank, name, and director."""

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
def _generate_review(**kwargs):
    project_id = kwargs["dag_run"].conf.get("project_id")
    region = kwargs["dag_run"].conf.get("region")
    stg_dataset_name = kwargs["dag_run"].conf.get("stg_dataset_name")
    stg_ai_dataset_name = kwargs["dag_run"].conf.get("stg_ai_dataset_name")
    remote_models = kwargs["dag_run"].conf.get("remote_models")

    bq_client = bigquery.Client(project=project_id, location=region)

    # create remote connection
    remote_sql = (f"""
    create or replace model {remote_models}.gemini_pro
      remote with connection `projects/composer-mahek-daisy/locations/us-central1/connections/us-central1-ai`
      options (endpoint = 'gemini-pro')""")
    
    bq_client.query(remote_sql).result()


    # MOVIE REVIEW GENERATION
    # creating the prompt
    prompt_sql = (f"""
    declare prompt_query STRING default "Generate a brief movie review based on the given rank, movie name, and director. The review should consider the movie's significance, its themes, or its cultural impact. Return the output as a JSON object, including the movie rank, name, director, and the generated review."
    """)
    
    bq_client.query(prompt_sql).result()

    # generating the data
    review_data_sql = (f""" 
    create or replace table {stg_ai_dataset_name}.movie_review_predictions_raw_full as
    select *
    from ML.generate_text(
    model {remote_models}.gemini_pro,
    (
        select concat(prompt_query, to_json_string(json_object("rank", rank, "name", name, "director", director))) as prompt
        from {stg_dataset_name}.Film
        order by rank
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
    )
    """)

    bq_client.query(review_data_sql).result()


    # format the JSON
    json_format_sql = (f"""
    create or replace table {stg_ai_dataset_name}.movie_review_predictions_formatted_full as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\n', '')) as ml_generate_text_llm_result
    from {stg_ai_dataset_name}.movie_review_predictions_raw_full
    """)

    bq_client.query(json_format_sql).result()

    # add column to the staging table
    review_column_sql = (f"""
    alter table {stg_dataset_name}.Film add column review string
    """)

    bq_client.query(review_column_sql).result()

    # update data in staging table
    insert_livability_sql = (f"""
    update {stg_dataset_name}.Film f
    set review = json_value(p.ml_generate_text_llm_result, '$.review')
    from {stg_ai_dataset_name}.movie_review_predictions_formatted_full p
    where f.name = json_value(p.ml_generate_text_llm_result, '$.name')
    """)

    bq_client.query(insert_livability_sql).result()

    # update the data source
    data_source_sql = (f"""
    update {stg_dataset_name}.Film
    set data_source = 'kaggle_ai where 
    review is not null
    """)

    bq_client.query(data_source_sql).result()


with models.DAG(
    "finalproj-generate-review",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    generate_review = _generate_review()

    # task dependencies
    generate_review