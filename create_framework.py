import subprocess
import snowflake.connector
import os
from dotenv import load_dotenv

class user_connection:
    def __init__(self,account,username,password,warehouse,database,schema,role):
        self.account=account
        self.username=username
        self.password=password
        self.warehouse=warehouse
        self.database=database
        self.schema=schema
        self.role=role
    def connection_integration(self):
        conn = snowflake.connector.connect(account=self.account,user=self.username,password=self.password, warehouse=self.warehouse, database=self.database,schema=self.schema,role=self.role)
        cursor = conn.cursor()
        return cursor
    def source_generator(self):  
        bashCommand = ['dbt','--quiet','run-operation','generate_source','--args','{"schema_name": "STG", "include_database": true , "database_name": "ENTAIN_POC" , "generate_columns": true , "include_schema": true}']
    # Output file
        outputFile = "models/INTEGRATIONLAYER/source_landed.yml"
        
        print(" ".join(bashCommand))
    # Run the bash command and redirect output to the file
        with open(outputFile, 'a') as f:
            subprocess.run(bashCommand, stdout=f, shell=True, text=True, encoding='utf-8')


    # def INTEGRATIONLAYER_generator(self):  
    #     bashCommand = ['dbt','--quiet','run-operation','generate_source','--args','{"schema_name": "DBO", "include_database": true , "database_name": "ENTAIN_POC" , "generate_columns": true , "include_schema": true  }']
    # # Output file
    #     outputFile = "models/PRESENTATIONLAYER/source_landed.yml"
     
    #     print(" ".join(bashCommand))
    # # Run the bash command and redirect output to the file
    #     with open(outputFile, 'w') as f:
    #         subprocess.run(bashCommand, stdout=f, shell=True, text=True, encoding='utf-8')


    # def INTEGRATIONLAYER(self,cursor):   
    #     for row in cursor.fetchall():
    #         id = row[0]
    #         bashCommand = ["dbt", "--quiet", "run-operation", "INTEGRATIONA_LAYER", "--args", "{'query_id': " + str(id) + " }"]
    #     # Output file
    #         model_name = row[2]
    #         outputFile = "models/INTEGRATIONLAYER/""" + model_name + ""+".sql"
           
    #         print(" ".join(bashCommand))
    #     # Run the bash command and redirect output to the file
    #         with open(outputFile, 'w') as f:
    #             subprocess.run(bashCommand, stdout=f, shell=True, text=True, encoding='utf-8')
    #     cursor.close()
    def PRESENTATIONLAYER(self,cursor):
        for row in cursor.fetchall():
            id = row[0]
            bashCommand = ["dbt", "--quiet", "run-operation", "model_generation", "--args", "{'query_id': " + str(id) + " }"]
        # Output file
            model_name = row[2]
            outputFile = "models/PRESENTATIONLAYER/"+"" + model_name + ""+".sql"
           
            print(" ".join(bashCommand))
        # Run the bash command and redirect output to the file
            with open(outputFile, 'w') as f:
                subprocess.run(bashCommand, stdout=f, shell=True, text=True, encoding='utf-8')
        cursor.close()

# Create a cursor to execute SQL queries
load_dotenv()

account = os.getenv('ACCOUNT_NAME')
username = os.getenv('USER_NAME')
password = os.getenv('PASSWORD')
warehouse = os.getenv('WAREHOUSE')
database = os.getenv('DATABASE')
schema = os.getenv('SCHEMA')
role = os.getenv('ROLE')


input_passing=user_connection(account ,username,password ,warehouse ,database,schema,role)
# INTEGRATIONLAYER_QUERY= '''SELECT * FROM ENTAIN_POC.META.METADATACONTROLMODEL WHERE "ObjectType" = 'TABLE' AND "IsModelCreated" = 'NOT CREATED' '''
# cursor=input_passing.connection_integration().execute(INTEGRATIONLAYER_QUERY)
input_passing.source_generator()
# input_passing.INTEGRATIONLAYER(cursor)

PRESENTATION_QUERY= '''SELECT * FROM ENTAIN_POC.META.METADATACONTROLMODEL WHERE "ObjectType" = 'Table'  AND "IsModelCreated" = 'NOT CREATED' '''
cursor=input_passing.connection_integration().execute(PRESENTATION_QUERY)
# input_passing.INTEGRATIONLAYER_generator()

input_passing.PRESENTATIONLAYER(cursor)