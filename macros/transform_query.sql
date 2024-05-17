
{% macro schema_table(match) %}
    {% set type_join = match.group(1) %}
    {% set schema_name = match.group(2) %}
    {% set table_name = match.group(3) %}
    {% set source_name = "{{ source('" ~ schema_name ~ "','" ~ table_name|upper ~ "') }} " %}
    {% if type_join in ['from', 'FROM'] %}
        {% do return("from "~ source_name ) %} 
    {% else %}
        {% do return("join  "~ source_name ) %} 
    {% endif %}
{% endmacro %}

{% macro database_schema_table(match) %}
    {% set type_join = match.group(1) %}
    {% set database_name = match.group(2) %}
    {% set schema_name = match.group(3) %}
    {% set table_name = match.group(4) %}
    {% set source_name = "{{ source('" ~ schema_name ~ "','" ~ table_name|upper ~ "') }} " %}
    {% if type_join in ['from', 'FROM'] %}
        {% do return("from "~ source_name ) %} 
    {% else %}
        {% do return("join "~ source_name ) %} 
    {% endif %}
{% endmacro %}
 

{% macro reference_table(match) %}
    {% set type_join = match.group(1) %}
    {% set model_name = match.group(2) %}

    {% set model_list = [] %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {%- do model_list.append(node.alias) -%}
        {% endif %}
    {% endfor %}
  

    {% set ref_name = "{{ ref('" ~ model_name ~ "') }} " %}
    {% if type_join in ['from', 'FROM'] %}
        {% if model_name in model_list  %}
            {% do return("from "~ ref_name ) %}
        {% else %}
             {% do return("from "~ model_name~" " ) %}
        {% endif %}
    {% else %}
        {% if model_name in model_list  %}
            {% do return("join "~ ref_name ) %}
        {% else %}
             {% do return("join "~ model_name~" " ) %}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro Tranformation_ref_source(sql) %}
  {{ return(adapter.dispatch('Tranformation_ref_source', 'ownmacro')(sql)) }}
{% endmacro %}

{% macro default__Tranformation_ref_source(sql) %}
    {%- set re = modules.re -%}
    {% set table_from_sche_table_pattern = '(from|join)\\s+(\\w+)\\.(\\w+)[\\s+]*' %}
    {% set table_from_database_schema_table_pattern = '(from|join)\\s+(\\w+)\\.(\\w+)\\.(\\w+)[\\s+]*' %}
    {% set table_from_reference_model = '(from|join)\\s+(\\w+)[\\s+]*' %}
    {% set source__database_transform = re.sub(table_from_database_schema_table_pattern,database_schema_table , sql, flags=re.IGNORECASE) %}
    {% set source_transform = re.sub(table_from_sche_table_pattern, schema_table, source__database_transform, flags=re.IGNORECASE) %}
    {% set reference_transform = re.sub(table_from_reference_model, reference_table, source_transform, flags=re.IGNORECASE) %}
     {% if reference_transform.find("with") == -1 and reference_transform.find("WITH ") == -1 %}
        {% set result_string = reference_transform|replace("from", ",'{{ invocation_id }}' as Invocation_Id from ",1) %}
    {% else %}
        {% set x = reference_transform %}
        {% set old_substring = "from" %}
        {% set new_substring = ",'{{ invocation_id }}' as DBT_InvocationId from " %}
        {% set x_reversed = x | reverse %}
        {% set old_substring_reversed = old_substring | reverse %}
        {% set new_substring_reversed = new_substring | reverse %}
        {% set result_string_reversed = x_reversed | replace(old_substring_reversed, new_substring_reversed,1) %}
        {% set result_string = result_string_reversed | reverse %}
    {% endif %}
    {% do return(result_string) %}
{% endmacro %}