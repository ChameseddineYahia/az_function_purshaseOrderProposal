import logging

import azure.functions as func
import pysftp
import logging
import sys
from sql_statements import SQL_STATEMENTS
from dotenv import load_dotenv
from config import sftp_config , azure_blob_config , connection_config , flow_config,azure_table_config,sql_server_config
from schema.XXAIH_POS_SO_IMPORTSchema import XXAIH_POS_SO_IMPORTSchema
from connectors.sftpProcessing import SFTPProcessing
from processing.processCSV import ProcessCSV
from processing.schemaValidation import SchemaValidation
from connectors.oracleDB import OracleDBConnector , SQLStatementExecutor
from processing.ErrorHandler import ErrorHandler
from connectors.azureBlobStorage import AzureBlobProcessor
from connectors.azureTableStorage import AzureTableStorage
from connectors.MsServerSQL import SQLServerDatabase
import pandas as pd

load_dotenv()


def main(req: func.HttpRequest) -> func.HttpResponse:
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
            
    regex_pattern = rf'{flow_config["regex_pattern"]}'



    # Configure the logging module
    logging.basicConfig(filename='your_log_file.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')  # Log to a file
    console_handler = logging.StreamHandler(sys.stdout)  # Log to the console
    console_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(console_handler)



    try:
            # init classes

        errHandler = ErrorHandler()
        schema = XXAIH_POS_SO_IMPORTSchema()
        sftp = SFTPProcessing(**sftp_config)
        processing = ProcessCSV()
        schemaValid = SchemaValidation(schema)

        azblob = AzureBlobProcessor(**azure_blob_config)
     
        

        #begin
        files = sftp.read_files(regex_pattern=regex_pattern)

        for file in files:
            table_rows = []
            logging.info(f"filename : {file[0]}")
            df_info = processing.process(file[1])
            sqlserver = SQLServerDatabase(**sql_server_config, table_name='test',
    columns_formatted=df_info["columns_normal"],
    columns_normal=df_info["columns_normal"])
            required_result = schemaValid.handle_required_errors(data=df_info)
            errHandler.add_df_required(required_result["invalid_rows"])
            type_result = schemaValid.handle_type_errors(data=df_info)
            errHandler.add_df_type(type_result["invalid_rows"])
            result_insert = sqlserver.insert_data(type_result["all_rows"])
            if result_insert : 
                logging.info(f'successful rows {result_insert["successful_rows"]}') 
                logging.info(f'problematic_rows {result_insert["problematic_rows"]}')
                errHandler.add_df_db(result_insert["all_rows"])
            #az blob storage
            output_file = errHandler.generate_main_csv(file[0])
            azblob.push_files_to_blob([(file[0] , output_file)])
            return func.HttpResponse("Request processed successfully.", status_code=200)

            
        
    except Exception as e:
                logging.warning(f"Warning processing rows: {e}")
                return func.HttpResponse("Request processed failed.", status_code=500)
    


          