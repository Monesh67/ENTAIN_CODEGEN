import re

def transform_query(sql,list):
    # Regular expression pattern to match table references
    table_from_sche_table_pattern = r''(from|join)\\s+(\\w+)\\.(\\w+)[\\s+]*''
    table_from_database_schema_table_pattern = r''(from|join)\\s+(\\w+)\\.(\\w+)\\.(\\w+)[\\s+]*''
    table_from_reference_model =r''(from|join)\\s+(\\w+)[\\s+)]''
    def schema_table(match):
        if match.group(1):
            type_join = match.group(1)
            schema_name =  match.group(2)
            table_name = match.group(3)
            patter_compin =f"{{source(''{schema_name}'',''{table_name.upper()}'')}}"
            patter_compin_2=f''{{{patter_compin}}} ''
            if type_join in [''from'',''FROM'']:
                patter_compin_3=f''from {patter_compin_2} ''
            else:
                patter_compin_3=f''join {patter_compin_2} ''
            return patter_compin_3
    def database_schema_table(match):
        if match.group(1):
            type_join = match.group(1)
            database_name =  match.group(2)
            print(database_name)
            schema_name =  match.group(3)
            table_name = match.group(4)
            patter_compin =f"{{source(''{schema_name}'',''{table_name.upper()}'')}}"
            patter_compin_2=f''{{{patter_compin}}} ''
            if type_join in [''from'',''FROM'']:
                patter_compin_3=f''from {patter_compin_2} ''
            else:
                patter_compin_3=f''join {patter_compin_2} ''
            return patter_compin_3
    def single_name(match):
        if match.group(1):
            type_join = match.group(1)
            model_name =  match.group(2)
            if model_name in list:
                print(model_name)
                patter_compin =f"{{ref(''{model_name}'')}}"
                patter_compin_2=f''{{{patter_compin}}}''
                if type_join in [''from'',''FROM'']:
                    patter_compin_3=f''from {patter_compin_2} ''
                else:
                    patter_compin_3=f''join {patter_compin_2} ''
                return patter_compin_3
            else:
                patter_compin =f"{model_name}"
               
                if type_join in [''from'',''FROM'']:
                    patter_compin_3=f''from {patter_compin} ''
                else:
                    patter_compin_3=f''join {patter_compin} ''
                return patter_compin_3
                
    # transformed_sql = re.sub(table_from_sche_table_pattern, schema_table, sql, flags=re.IGNORECASE)
    
    source__database_transform = re.sub(table_from_database_schema_table_pattern, database_schema_table,  sql , flags=re.IGNORECASE)
    source_transform = re.sub(table_from_sche_table_pattern, schema_table, source__database_transform, flags=re.IGNORECASE)
    reference_transform = re.sub(table_from_reference_model, single_name,  source_transform , flags=re.IGNORECASE)
    if reference_transform.find("with ") == -1 and reference_transform.find("WITH ")== -1:
        result_string = reference_transform.replace("from", ",''{{invocation_id}}'' as DBT_InvocationId from ")
    else:

        x=reference_transform
        
        old_substring = "from"
    
        new_substring = ",''{{invocation_id}}'' as DBT_InvocationId from "
        
        x_reversed = x[::-1]
        
        old_substring_reversed = old_substring[::-1]
        new_substring_reversed = new_substring[::-1]
        

        result_string_reversed = x_reversed.replace(
                            old_substring_reversed,
                            new_substring_reversed,
                            1)
        

        result_string = result_string_reversed[::-1]
        
    return result_string