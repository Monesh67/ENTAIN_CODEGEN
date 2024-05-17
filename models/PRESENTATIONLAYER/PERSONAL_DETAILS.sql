
{{ config(materialized='incremental',unique_key=[
  "RollNo"
])}}

SELECT * ,'{{ invocation_id }}' as Invocation_Id from  {{ source('STG','PERSONAL_DETAILS') }} 

{% if is_incremental() %}
where ModifiedDate> (select max(ModifiedDate) from {{this}})
{% endif %}

