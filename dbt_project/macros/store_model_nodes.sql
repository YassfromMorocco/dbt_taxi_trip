/*
  --This macro store all model result in 2 tables :
  1-a temporary table where are stored only the model results obtained with the last dbt test
  2-a historical table where all historical test model are stored

  Inputs:
  --a results variable where are stored dbt logs

  requirements:
  --add "{{ store_test_results(results) }}" to an on-run-end: block in dbt_project.yml 
*/

{% macro store_model_nodes(results) %}
  {%- set model_results = [] -%}

  {%- for result in results if result.node.resource_type == 'model' and result.status != 'error'-%}
    {%- set model_results = model_results.append(result) -%}
  {%- endfor -%}

  {% if model_results|length == 0 -%}
    {{ log("store_model_results found no model results to process.") if execute }}
    {{ return('') }}
  {% endif -%}

  {% set temp_model_table = var('dbt_project')  ~ '.' ~ var('dbt_test_dataset')  ~ '.model_results_temp'%}
  {%- set history_model_table = var('dbt_project')  ~ '.' ~ var('dbt_test_dataset')  ~ '.model_results_historical'%}

  /*
  --logs to know where data is stored
  */ 
  {%- set temp_model_table -%} {{ target.schema }}.model_results_temp {%- endset -%}
  {%- set history_model_table -%} {{ target.schema }}.model_results_historical {%- endset -%}

  /*
  --create temporary table to store test logs
  */ 

  CREATE OR REPLACE TABLE `{{ temp_model_table }}` AS (

    {%- for model in model_results %}
      SELECT
        CAST('{{ model.node.name }}' AS STRING) AS model_name,
        CAST('{{ model.status }}' AS STRING) AS model_status,
        CAST('{{ model.node.schema }}' AS STRING) AS dataset,
        CAST('{{ model.node.database }}' AS STRING) AS project,
        CAST('{{ model.node.fqn[1] }}' AS STRING) AS directory,
        CAST('{{ model.node.config.materialized }}' AS STRING) AS model_materialization,
        CAST('{{ process_refs(model.node.refs) }}' AS STRING) AS model_refs,
        CAST('{{ process_refs(model.node.sources, is_src=true) }}' AS STRING) AS model_source,
        CAST('{{ model.timing[1].started_at }}' AS STRING) AS execute_start_at,
        CAST('{{ model.execution_time }}' AS STRING) AS model_execution_time_seconds,
        CAST('{{ model.adapter_response.bytes_processed }}' AS STRING) AS bytes_processed,
        CAST('{{ model.adapter_response.rows_affected }}' AS STRING) AS rows_affected
      {{ "union all" if not loop.last }}

    {%- endfor %}
  );

  CREATE TABLE IF NOT EXISTS `{{ history_model_table }}` AS (
    SELECT 
    * 
    FROM `{{ temp_model_table }}`
    WHERE false
  );

  INSERT INTO `{{ history_model_table }}` 
    SELECT  
    * 
    FROM `{{ temp_model_table }}`
  ;

{% endmacro %}

/*
  return a comma delimited string of the models or sources were related to the test.
    e.g. dim_customers,fct_orders
  behaviour changes slightly with the is_src flag because:
    - models come through as [['model'], ['model_b']]
    - srcs come through as [['source','table'], ['source_b','table_b']]
 */

{% macro process_tags( tag_list ) %}
  {% set tags = [] %}
  {% if tag_list is defined and tag_list|length > 0 %}

      {% for tag in tag_list %}

        {{ tags.append(tag) }}

      {% endfor %}

      {{ return(tags|join(',')) }}

  {% else %}
      {{ return('') }}
  {% endif %}
{% endmacro %}
