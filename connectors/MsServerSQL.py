import pyodbc

class SQLServerDatabase:
    def __init__(self, sql_server, database, username, password, table_name, columns_formatted, columns_normal):
        # Set up the connection string
        self.connection_string = (
            f"DRIVER=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.3.so.2.1;Server={sql_server};Database={database};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            f"Uid={username};Pwd={password};"
        )
        self.table_name = table_name
        self.columns_formatted = columns_formatted
        self.columns_normal = columns_normal

        # Establish the database connection
        self.connection = pyodbc.connect(self.connection_string)
        self.cursor = self.connection.cursor()
        print("connection established !")


    def select_data(self, query):
        try:
            # Execute the SELECT query
            self.cursor.execute(query)

            # Fetch all rows and return the result
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            print(f"Error during SELECT operation: {e}")
        finally:
            self.cursor.close()

    def insert_data(self, data):
        successful_rows = []
        problematic_rows = []
        all_rows = []

        try:
            # Prepare the INSERT query with formatted columns
            insert_query = f"INSERT INTO {self.table_name} ({', '.join(self.columns_formatted)}) VALUES ({', '.join(['?'] * len(self.columns_normal))})"
            
            for row in data:
                try:
                    # Execute the INSERT query for each row of data
                    self.cursor.execute(insert_query, row)
                    self.connection.commit()
                    successful_rows.append(row)
                except Exception as e:
                    # If an error occurs, add the row to problematic_rows
                    problematic_rows.append({"row": row, "error": str(e)})
                finally:
                    all_rows.append(row)

            print("Insert operation successful.")
            return {"successful_rows": successful_rows, "problematic_rows": problematic_rows, "all_rows": all_rows}

        except Exception as e:
            # Rollback in case of a global error
            self.connection.rollback()
            print(f"Error during INSERT operation: {e}")
            return {"successful_rows": successful_rows, "problematic_rows": problematic_rows, "all_rows": all_rows}

        finally:
            self.cursor.close()

    def close_connection(self):
        # Close the database connection
        self.connection.close()
    def close_connection(self):
        # Close the database connection
        self.connection.close()