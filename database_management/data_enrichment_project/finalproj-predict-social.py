# Life Expectancy and Population Predictions subdag
"""A dag that enhances the staging Social table by predicting the life expectancy and population in 2100 and 3000."""
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
def _predict_life_expectancy(**kwargs):
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


    # LIFE EXPECTANCY PREDICTIONS
    # Predicting life_expectancy rates in 2100 and 3000
    # creating the prompt
    prompt_sql = (f"""
    declare prompt_query STRING default "Predict the life expectancy rate in the year 2100 and 3000 for each country using extensive historical data, advanced statistical modeling, and consideration of various socio-economic factors. Incorporate the given country code, current population, and current life expectancy into the analysis. Return the output as only a JSON, including the country code, local name, current population, current life expectancy, and the predicted life expectancy rate for the year 2100 and 3000. Do not include a disclaimer or explanation as to how you got these results in the output."
    """)
    
    bq_client.query(prompt_sql).result()

    # generating the data
    life_expectancy_data_sql = (f""" 
    create or replace table {stg_ai_dataset_name}.life_expectancy_predictions_full as
    select *
    from ML.generate_text(
    model {remote_models}.gemini_pro,
    (
        select concat(prompt_query, to_json_string(json_object("country_code", code, "population", population, "life_expectancy", cast(life_expectancy as float64), "local_name", local_name))) as prompt
        from {stg_dataset_name}.Social
        order by code
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
    )
    """)

    bq_client.query(life_expectancy_data_sql).result()


    # format the JSON
    json_format_sql = (f"""
    create or replace table {stg_ai_dataset_name}.life_expectancy_predictions_formatted_full as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\n', '')) as ml_generate_text_llm_result
    from {stg_ai_dataset_name}.life_expectancy_predictions_full
    """)

    bq_client.query(json_format_sql).result()


    # add the life_expectancy columns to the table
    life_expectancy_2100_column_sql = (f"""
    alter table {stg_dataset_name}.Social add column life_expectancy_2100 float64;
    """)

    life_expectancy_3000_column_sql = (f"""
    alter table {stg_dataset_name}.Social add column life_expectancy_3000 float64;
    """)

    bq_client.query(life_expectancy_2100_column_sql).result()
    bq_client.query(life_expectancy_3000_column_sql).result()


    # update data in staging table
    # life_expectancy_2100
    insert_life_expectancy_2100_sql = (f"""
    update {stg_dataset_name}.Social set life_expectancy_2100 =
    (select cast(json_value(ml_generate_text_llm_result, '$.predicted_life_expectancy_2100') as float64)
    from {stg_ai_dataset_name}.life_expectancy_predictions_formatted_full
    where code = json_value(ml_generate_text_llm_result, '$.country_code'))
    where 1=1
    """)

    # life_expectancy_3000
    insert_life_expectancy_3000_sql = (f"""
    update {stg_dataset_name}.Social set life_expectancy_3000 =
    (select cast(json_value(ml_generate_text_llm_result, '$.predicted_life_expectancy_3000') as float64)
    from {stg_ai_dataset_name}.life_expectancy_predictions_formatted_full
    where code = json_value(ml_generate_text_llm_result, '$.country_code'))
    where 1=1
    """)

    bq_client.query(insert_life_expectancy_2100_sql).result()
    bq_client.query(insert_life_expectancy_3000_sql).result()


@task(trigger_rule="all_done")
def _predict_population(**kwargs):
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


    # POPULATION PREDICTIONS
    # Predicting population in 2100 and 3000
    # creating the prompt
    prompt_sql = (f"""
    declare prompt_query STRING default "Predict the population in the year 2100 and 3000 for the given country using extensive historical data, advanced statistical modeling, and consideration of various socio-economic factors. Incorporate the given country code, current population, and current life expectancy into the analysis. Return the output as only a JSON, including the country code, local name, current population, and the predicted population for the year 2100 and 3000. Do not include a disclaimer, explanation or note as to how you got these results in the output. Name the predicted population columns population_2100 and population_3000."
    """)
    
    bq_client.query(prompt_sql).result()

    # generating the data
    population_data_sql = (f""" 
    create or replace table {stg_ai_dataset_name}.population_predictions_raw_full as
    select *
    from ML.generate_text(
    model {remote_models}.gemini_pro,
    (
        select concat(prompt_query, to_json_string(json_object("country_code", code, "population", CAST(population AS FLOAT64), "life_expectancy", life_expectancy, "local_name", local_name))) as prompt
        from {stg_dataset_name}.Social
        group by code, local_name, population, life_expectancy
        order by code
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
    )
    """)

    bq_client.query(population_data_sql).result()


    # format the JSON
    json_format_sql = (f"""
    create or replace table {stg_ai_dataset_name}.population_predictions_formatted_full as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\n', '')) as ml_generate_text_llm_result
    from {stg_ai_dataset_name}.population_predictions_raw_full
    """)

    bq_client.query(json_format_sql).result()


    # add the population columns to the table
    population_2100_column_sql = (f"""
    alter table {stg_dataset_name}.Social add column population_2100 float64;
    """)

    population_3000_column_sql = (f"""
    alter table {stg_dataset_name}.Social add column population_3000 float64;
    """)

    bq_client.query(population_2100_column_sql).result()
    bq_client.query(population_3000_column_sql).result()


    # update data in staging table
    # population_2100
    insert_population_2100_sql = (f"""
    update {stg_dataset_name}.Social set population_2100 =
    (select cast(json_value(ml_generate_text_llm_result, '$.population_2100') as float64)
    from {stg_ai_dataset_name}.population_predictions_formatted_full
    where code = json_value(ml_generate_text_llm_result, '$.country_code'))
    where 1=1
    """)

    # population_3000
    insert_population_3000_sql = (f"""
    update {stg_dataset_name}.Social set population_3000 =
    (select cast(json_value(ml_generate_text_llm_result, '$.population_3000') as float64)
    from {stg_ai_dataset_name}.population_predictions_formatted_full
    where code = json_value(ml_generate_text_llm_result, '$.country_code'))
    where 1=1
    """)

    bq_client.query(insert_population_2100_sql).result()
    bq_client.query(insert_population_3000_sql).result()

    # update the data source
    data_source_sql = (f"""
    update {stg_dataset_name}.Social
    set data_source = 'bird_ai' where 
    life_expectancy_2100 is not null 
    or life_expectancy_3000 is not null
    or population_2100 is not null
    or population_3000 is not null
    """)

    bq_client.query(data_source_sql).result()


with models.DAG(
    "finalproj-predict-social",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    predict_life_expectancy = _predict_life_expectancy()
    predict_population = _predict_population()

    # task dependencies
    predict_life_expectancy
    predict_population