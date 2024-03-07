import pandas as pd
import io
import logging

class ErrorHandler():
    def __init__(self):
        # Create a DataFrame for errors with the current date and time
        self.df_required = pd.DataFrame()
        # Create a DataFrame for success with the current date and time
        self.df_type = pd.DataFrame()
        self.df_db = pd.DataFrame()
        self.concatenated_df = pd.DataFrame()

    def add_df_required(self,rows):
        new_df = pd.DataFrame(rows)
        self.df_required = pd.concat([self.df_required, new_df], ignore_index=True)
        

    def add_df_type(self,rows):
        new_df = pd.DataFrame(rows)
        self.df_type =  pd.concat([ self.df_type , new_df])
    
    def add_df_db(self,rows):
        new_df = pd.DataFrame(rows)
        self.df_db =  pd.concat([ self.df_db , new_df])
        
    def generate_error_rows(self):
        filtered_rows = self.concatenated_df[self.concatenated_df['Status'].str.contains('not inserted', case=False, na=False)]

        # Convert filtered DataFrame to a list of dictionaries
        list_of_dicts = filtered_rows.to_dict(orient='records')
        return list_of_dicts
    
    def generate_main_csv(self,filename):
        memory_file = io.BytesIO()
        common_columns = set(self.df_db.columns) & set(self.df_type.columns)
        if common_columns:
            columns_without_status = [column for column in common_columns if column != "Status"]
            # concatenated_df = pd.concat([self.df_db,self.df_type], ignore_index=True)
            concatenated_df = pd.merge(self.df_db,self.df_type,how="left" , on=columns_without_status)
            # Concatenate the content of the "status" column
            if "Status_x" in concatenated_df.columns.values.tolist() and "Status_y" in concatenated_df.columns.values.tolist():
                print("first" , concatenated_df)
                concatenated_df["Status"] = concatenated_df["Status_x"].fillna("").astype(str) + concatenated_df["Status_y"].fillna("").astype(str)


                # Drop the individual "status" columns if needed
                concatenated_df.drop(["Status_x", "Status_y"], axis=1, inplace=True)
                print("second" , concatenated_df)
            concatenated_df = pd.concat([concatenated_df,self.df_required], ignore_index=True)
        else :
            concatenated_df = pd.concat([self.df_db,self.df_type], ignore_index=True)
        self.concatenated_df = concatenated_df
        concatenated_df.to_csv(memory_file, index=False)
        concatenated_df.to_csv(filename, index=False)
        memory_file.seek(0)
        return memory_file