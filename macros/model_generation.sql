
{% macro model_generation(query_id) %}
  {{ return(adapter.dispatch('model_generation', 'ownmacro')(query_id)) }}
{% endmacro %}

{% macro default__model_generation(query_id) %}


{% set models = [] %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {%- do models.append(node.alias) -%}
        {% endif %}
    {% endfor %}

{% set schema_table_name %}
  select "DbtControlId","SQLQuery","UniqueColumn","HWMColumn" from ENTAIN_POC.META.METADATACONTROLMODEL where "DbtControlId" = {{query_id}}
{% endset %}
{% if execute %}

{% set result_schema_table = run_query(schema_table_name) %}
{% set sql_query = Tranformation_ref_source(result_schema_table[0][1]) %}
{% set  materialized = 'incremental' %}
{% set  UNIQUE_KEY = result_schema_table[0][2] %}
{% set  model_incr_cln = result_schema_table[0][3] %}
{% set  sql_incr_cln =  result_schema_table[0][3] %}


{% endif %}

{% set update_stmt %}
UPDATE ENTAIN_POC.META.METADATACONTROLMODEL SET "IsModelCreated"= 'ÇREATED' WHERE "DbtControlId" = {{query_id}}
{% endset %}

{% do run_query(update_stmt) %}

{% set SQL_model =sql_query %}
{% set base_model_sql %}
{{ "{{ config(materialized='" ~ materialized ~ "',unique_key=" ~ UNIQUE_KEY ~ ")}}" }}

{{SQL_model }}

{{ "{% if is_incremental() %}"}}
{{"where " ~ sql_incr_cln ~ "> (select max(" ~ model_incr_cln ~") from {{this}})" }}
{{"{% endif %}"}}
{% endset %}



{% if execute %}
{{ print(base_model_sql) }}
{% do return(base_model_sql) %}

{% endif %}

{% endmacro %}
