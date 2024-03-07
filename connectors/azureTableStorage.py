from azure.data.tables import TableServiceClient, TableClient 
from azure.core.exceptions import ResourceExistsError
from azure.core.credentials import AzureNamedKeyCredential
import logging
from config import flow_config
import uuid


class AzureTableStorage:
    def __init__(self, account_name , account_key ,azure_table_name="", flow_name=""):
        self.azure_table_config = {"account_name":account_name , "account_key":account_key , "azure_table_name" :azure_table_name , "flow_name" : flow_name}
        azure_credential = AzureNamedKeyCredential(account_name, account_key)
        table_service_client = TableServiceClient(endpoint=f"https://{account_name}.table.core.windows.net", credential=azure_credential)
        self.chunk_size = 100
        try:
            table_client = table_service_client.create_table(azure_table_name)
            self.table_client = table_client
            logging.info("well connected ! table created ! ")
        except ResourceExistsError:
            table_client = table_service_client.get_table_client(azure_table_name) 
            self.table_client = table_client
            logging.error("well connected ! table Already Exists ! ")
            
            
        
    def create_file_entity(self,PartitionKey , RowKey , ErrorResult , Result , Status):
        return {"PartitionKey" : PartitionKey , "RowKey" : RowKey , "ErrorResult":ErrorResult ,"Result" : Result , "Status" : Status }
    def create_row_entity(self,ErrorRows= [],file_name=""):
        result = []
        for row in ErrorRows:
            result.append({"PartitionKey":flow_config["flow_name"] , "RowKey":uuid.uuid4() , "UID":row[flow_config["primary_key"]],"FileName" : file_name ,  "Error":row["Status"]})
        return result
    def chunks(self,lst, chunk_size):
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]
    def create_row_entity_batch(selfself,ErrorRows= [],file_name=""):
        result = []
        for row in ErrorRows:
            result.append(("upsert" , {"PartitionKey":flow_config["flow_name"] , "RowKey":str(uuid.uuid4()) , "UID":row[flow_config["primary_key"]],"FileName" : file_name ,  "Error":row["Status"]}))
        return result
    def insertion(self , entities = []):
        for entity in entities:
            try:
                print(entity)
                self.table_client.create_entity( entity=entity )
            except Exception as e :
                logging.error(f"error table insertion row {e}")
    def insertion_batch(self , entities = []):
            list_chunks = list(self.chunks(entities, self.chunk_size))
            for chunk in list_chunks:
                 self.table_client.submit_transaction( chunk)

