import cx_Oracle
from marshmallow import ValidationError
import logging
from datetime import datetime


class OracleDBConnector:
    def __init__(self, username, password, host, port, service_name):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name

    def connect(self):
        connection_str = f"{self.username}/{self.password}@{self.host}:{self.port}/{self.service_name}"
        connection = cx_Oracle.connect(connection_str)
        cursor = connection.cursor()
        return connection, cursor


class SQLStatementExecutor:
    def __init__(self, table_name, columns_formatted, columns_normal):
        self.table_name = table_name
        self.columns_formatted = columns_formatted
        self.columns_normal = columns_normal

    def format_sql_insert(self, rows):
        sql_query = f""" 
        INSERT INTO {self.table_name} ({", ".join(self.columns_normal)}) VALUES ({self.columns_formatted})
        """
        return sql_query, rows

    def execute_sql_insert(self, connection, cursor, rows):

            logging.warning(f"WARNING: file contains {len(rows)} rows")
            if len(rows) > 0:
                sql_query, rows = self.format_sql_insert(rows[:-1])

                logging.info(f"INFO: file contains {len(rows)} rows")
                cursor.executemany(sql_query, rows, batcherrors=True)
                batch_errors = cursor.getbatcherrors()
                logging.warning(f"WARNING: number of errors is {len(batch_errors)}: {batch_errors}")

                successful_rows = []
                problematic_rows = []
                all_rows = []
                error_indices = []
                

                if batch_errors:
                    for index, error in enumerate(batch_errors):
   
                        problematic_row = rows[error.offset]
                        problematic_rows.append( {**problematic_row, **{"Status": f"Not Inserted {error}"}})
                        all_rows.append({**problematic_row, **{"Status": f"Not Inserted {error}"}})
                  
                    logging.info(f"error offset , {str([error.offset for error in batch_errors])}")
                    error_indices = [error.offset for error in batch_errors]
                
                successful_rows =  [{**row, **{"Status": f"Inserted"}}  for i, row in enumerate(rows) if i not in error_indices]
                all_rows = all_rows  + successful_rows
                    

                connection.commit()
                return {"successful_rows" : successful_rows , "problematic_rows": problematic_rows , "all_rows" : all_rows}


         


