"""A subdag that inputs the missing country names and codes in the Master_Language staging table."""
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
def _countries_codes(**kwargs):
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


    # COUNTRY NAMES AND CODES CREATION
    # creating the prompt
    prompt_sql = (f"""
    declare prompt_query STRING default "For the given movie and its director, suggest a nationality based on the origin of the director's last name. Return the output as a JSON object, including only the movie's name, the director's name, and the nationality. If the nationality is indiscernable, write the nationality as 'Unknown'."
    """)
    
    bq_client.query(prompt_sql).result()

    # generating the data
    master_language_data_sql = (f""" 
    create or replace table {stg_ai_dataset_name}.country_predictions_raw_full as
    select *
    from ML.generate_text(
    model {remote_models}.gemini_pro,
    (
        select concat(prompt_query, to_json_string(json_object("language", language, "country_codes", country_codes, "countries", countries, "degree_of_endangerment", degree_of_endangerment))) as prompt
        from {stg_dataset_name}.Master_Language
        order by language
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
    )                         
    """)

    bq_client.query(master_language_data_sql).result()


    # format the JSON
    json_format_sql = (f"""
    create or replace table {stg_ai_dataset_name}.country_predictions_formatted_full as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\n', '')) as ml_generate_text_llm_result
    from {stg_ai_dataset_name}.country_predictions_raw_full                   
    """)

    bq_client.query(json_format_sql).result()


    # update data in staging table
    insert_country_codes_sql = (f"""
    update {stg_dataset_name}.Master_Language m
    set m.country_codes = json_value(p.ml_generate_text_llm_result, '$.country_codes')
    from {stg_ai_dataset_name}.country_predictions_formatted_full p
    where m.language = json_value(p.ml_generate_text_llm_result, '$.language') AND m.country_codes is null                   
    """)

    bq_client.query(insert_country_codes_sql).result()

    insert_country_names_sql = (f"""
    update {stg_dataset_name}.Master_Language m
    set m.countries = json_value(p.ml_generate_text_llm_result, '$.countries')
    from {stg_ai_dataset_name}.country_predictions_formatted_full p
    where m.language = json_value(p.ml_generate_text_llm_result, '$.language') AND m.countries is null                 
    """)

    bq_client.query(insert_country_names_sql).result()


    # update the data source
    data_source_sql1 = (f"""
    update {stg_dataset_name}.Master_Language
    set data_source = 'data.world_ai' where country_codes is not null AND data_source = 'data.world'                   l
    """)

    bq_client.query(data_source_sql1).result()

    data_source_sql2 = (f"""
    update {stg_dataset_name}.Master_Language
    set data_source = 'BIRD_ai' where countries is not null AND data_source = 'BIRD'                 l
    """)

    bq_client.query(data_source_sql2).result()



with models.DAG(
    "finalproj-countries-codes",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    countries_codes = _countries_codes()

    # task dependencies
    countries_codes