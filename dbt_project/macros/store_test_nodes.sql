/*
  --This macro store all test result in 2 tables :
  1-a temporary table where are stored only the test results obtained with the last dbt test
  2-a historical table where all historical test results are stored

  Inputs:
  --a results variable where are stored dbt logs

  requirements:
  --add "{{ store_test_results(results) }}" to an on-run-end: block in dbt_project.yml 
*/
{% macro store_test_nodes(results) %}
  {%- set test_results = [] -%}

  /*
  --filter on the test node to keep track of test results with dedicated tag only
  */
  {%- for result in results if result.node.resource_type == 'test' and result.status != 'error'-%}
    {%- set test_results = test_results.append(result) -%}
  {%- endfor -%}
  /*
  --if it is not a test the macro will return this message and stop the execution
  */
  {% if test_results|length == 0 -%}
    {{ log("store_test_results found no test results to process.", info = true) if execute }}
    {{ return('') }}
  {% endif -%}

  {{ log("store_test_results found " ~ test_results|length ~ "test results to process.", info = true) }}
  /*
  --creation of tables variables to store test results
  */
  {% set temp_test_table = var('dbt_test_dataset')  ~ '.test_results_temp'%}
  {%- set history_test_table = var('dbt_test_dataset')  ~ '.test_results_historical'%}

  /*
  --logs to know where data is stored
  */ 
  {{ log("Centralizing " ~ test_results|length ~ " test results in " + temp_test_table, info = true) }}
  {{ log("Centralizing " ~ test_results|length ~ " test results in " + history_test_table, info = true) }}

  /*
  --create temporary table to store test logs
  */ 
  CREATE OR REPLACE TABLE `{{ temp_test_table }}` AS (

    {%- for result in test_results %}
      {%- set test_name = '' -%}
      {%- set test_type = '' -%}
      {%- set column_name = '' -%}

      {%- if result.node.test_metadata is defined -%}
        {%- set test_name = result.node.test_metadata.name -%}
        {%- set test_type = 'generic' -%}
        
        {%- if test_name == 'relationships' -%}
          {%- set column_name = result.node.test_metadata.kwargs.field ~ ',' ~ result.node.test_metadata.kwargs.column_name -%}
        {%- else -%}
          {%- set column_name = result.node.test_metadata.kwargs.column_name -%}
        {%- endif -%}
      {%- elif result.node.name is defined -%}
        {%- set test_name = result.node.name -%}
        {%- set test_type = 'singular' -%}
      {%- endif %}

      SELECT
        CAST('{{ test_name }}' AS STRING) AS test_name,
        CAST('{{ result.status }}' AS STRING) AS result_status,
        CAST('{{ result.node.config.severity }}' AS STRING) AS test_severity_config,
        CAST('{{ column_name|escape }}' AS STRING) AS column_names,
        CAST('{{ process_refs(result.node.refs) }}' AS STRING) AS test_refs,
        CAST('{{ process_refs(result.node.sources, true) }}' AS STRING) AS test_sources,
        CAST('{{ test_type }}' AS STRING) AS test_type,
        CAST('{{ result.timing[0].started_at }}' AS STRING) AS compile_start_at,
        CAST('{{ result.timing[0].completed_at }}' AS STRING) AS compile_completed_at,
        CAST('{{ result.timing[1].started_at }}' AS STRING) AS execute_start_at,
        CAST('{{ result.timing[1].completed_at }}' AS STRING) AS execute_completed_at,
        CAST('{{ result.execution_time }}' AS STRING) AS model_execution_time_seconds,
        CAST('{{ target.name }}' AS STRING) AS dbt_cloud_target_name,
        CAST('{{ result.failures }}' AS STRING) AS nb_failures
      {{ "union all" if not loop.last }}

    {%- endfor %}
    );

  /*
  --create temporary historical table if not exists
  */ 
  CREATE TABLE IF NOT EXISTS `{{ history_test_table }}` AS (
    SELECT 
    * 
    FROM `{{ temp_test_table }}`
    WHERE FALSE
  );

  /*
  --store logs into historical table
  */ 
  INSERT INTO `{{ history_test_table }}` 
    SELECT  
    * 
    FROM `{{ temp_test_table }}`
  ;
{% endmacro %}

 /*
  return a comma delimited string of the models or sources were related to the test.
    e.g. dim_customers,fct_orders
  behaviour changes slightly with the is_src flag because:
    - models come through as [['model'], ['model_b']]
    - srcs come through as [['source','table'], ['source_b','table_b']]
 */

{% macro process_refs( ref_list, is_src=false ) %}
  {% set refs = [] %}

  {% if ref_list is defined and ref_list|length > 0 %}
    {% for ref in ref_list %}
      {% if is_src %}
        {{ refs.append(ref[1]) }}
      {% else %}
        {{ refs.append(ref[0]) }}
      {% endif %} 
    {% endfor %}

    {{ return(refs|join(',')) }}

  {% else %}
    {{ return('') }}
  {% endif %}
{% endmacro %}
