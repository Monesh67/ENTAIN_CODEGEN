{% macro parse_dbt_results(results) %}
    -- Create a list of parsed results
    {%- set parsed_results = [] %}
    -- Flatten results and add to list
    {% for run_result in results %}
        -- Convert the run result object to a simple dictionary
        {% set run_result_dict = run_result.to_dict() %}
        -- Get the underlying dbt graph node that was executed
        {% set excution_Time=run_result_dict.get('timing')[1] %}
        {% set node = run_result_dict.get('node') %}    
        {% set rows_affected = run_result_dict.get('adapter_response', {}).get('rows_affected', 0) %}
        {%- if not rows_affected -%}
            {% set rows_affected = 0 %}
        {%- endif -%}
        {% set run_status = run_result_dict.get('message') | replace("'", "\\'") %}
        {% set MODEL_CODE = node.get('raw_code') | replace("'", "\\'") %}
         {% if excution_Time | length  == 0 %}
            {% set STARTTIMEMODEL   = none  %} 
            {% set ENDTIMEMODEL  = none  %}
        {% else %}
            {% set STARTTIMEMODEL   = excution_Time.get('started_at') %} 
            {% set ENDTIMEMODEL  = excution_Time.get('completed_at') %} 
        {% endif %}
        {% set parsed_result_dict = {
                'result_id': invocation_id ~ '.' ~ node.get('unique_id'),
                'invocation_id': invocation_id,
                'unique_id': node.get('unique_id'),
                'database_name': node.get('database'),
                'schema_name': node.get('schema'),
                'name': node.get('name'),
                'resource_type': node.get('resource_type'),
                'status': run_result_dict.get('status'),
                'execution_time': run_result_dict.get('execution_time'),
                'rows_affected': rows_affected,
                'STARTTIMEMODEL': STARTTIMEMODEL,
                'ENDTIMEMODEL' :  ENDTIMEMODEL,
                'STATUS_LOG': run_status,
                'MODEL_CODE': MODEL_CODE
                }%}
        {% do parsed_results.append(parsed_result_dict) %}
    {% endfor %}
    {{ return(parsed_results) }}
{% endmacro %}
