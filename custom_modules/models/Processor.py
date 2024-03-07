from io import StringIO
from azure.storage.blob import BlobServiceClient
import pandas as pd
import numpy as np
class DataManip:
    """
    This class is used to manipulate data in Azure Blob Storage.
    It provides methods for downloading, uploading, and deleting files from Azure Blob Storage.
    It also provides methods for fetching mapping data from Azure Blob Storage and mapping dataframes.
    It also provides methods for cleaning dataframes.
    """
    def __init__(self, connection_string, container_name):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = container_name
    
    def get_mapping_dataframe(self,
                              mapping_blob_name:str
                              )->pd.DataFrame:
        """
        This function downloads a mapping file from Azure Blob Storage and 
        reads it into a pandas dataframe.
        """
        # Get a blob client for the mapping file
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=mapping_blob_name)

        # Download the content of the blob
        blob_content = blob_client.download_blob().content_as_text()

        # Read the content of the blob into a pandas dataframe
        df_mapping_columns = pd.read_csv(StringIO(blob_content), sep=";",dtype=str)

        return df_mapping_columns

    def fetch_mapping_data(
            self,
            sheet_name:str
            )->pd.DataFrame:
        """
        Fetches mapping data from Azure Blob Storage
        """
        blob_name = f"mapping/{sheet_name}.csv"
        blob_client = self.blob_service_client.get_blob_client(self.container_name, blob_name)
        downloaded_blob = blob_client.download_blob()
        content = pd.read_csv(StringIO(downloaded_blob.content_as_text()), sep=";")
        # Create a dictionary with the new row values
        new_row = {col: "." for col in content.columns}
        # Append the new row to the DataFrame TODO : change append to concat won't be supported in version
        content = content.append(new_row, ignore_index=True)
        return content

    def remove_duplicates(self, df, navision_codes):
        """Removes duplicate columns from the given data frame."""
        output_df = df[navision_codes]
        output_df = output_df.drop_duplicates(keep="last")
        return output_df

    def clean_dataframe(
            self,
            df:pd.DataFrame
            )->pd.DataFrame:
        """
        Apply strip() method to all string values and remove leading/trailing spaces from column names.
        """
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.rename(columns={col: col.strip() for col in df.columns})
        return df
    
    def merge_data_frames(
            self,
            df_data:pd.DataFrame,
            mapping_data:pd.DataFrame,
            column : pd.Series,
            MAPPING_BLOB_NAME : str
            ):
        """
        Merges the given DataFrame with the given mapping data.
        """
        left_on_columns = []
        right_on_columns = []
        COMMON_MAPPING_BLOB_NAMES = ["mapping/Structure export Client.csv", "mapping/Structure vente.csv", "mapping/Structure paiement.csv"]
        MOUVEMENT_STOCK_BLOB_NAMES= ["mapping/Structure Brouillon de sortie.csv","mapping/Structure Brouillon d'entrée.csv",'mapping/Structure Entée exceptionnelle.csv']
        if column["Mapping Sheet Name"] == "Valeur de dimension":
            left_on_columns = [
                str(column["champ Navision"]).replace("valeur", "attribut"),
                column["champ Navision"]
            ]
            right_on_columns = ["CODE grille dimension", "Code Navision"]
            merged_df = pd.merge(
                df_data,
                mapping_data[right_on_columns + ["Code cegid"]],
                how="left",
                indicator=True,
                left_on=left_on_columns,
                right_on=right_on_columns
            )

            merged_df = merged_df.replace(".", np.nan)

            merged_df.rename(
                columns={"Code cegid": column["champ Navision"]},
                inplace=True,
                errors="raise"
            )
        elif column["Mapping Sheet Name"] == "Famille niveau":
            left_on_columns = [column["champ Navision"], "CHAMP TECHNIQUE"]
            right_on_columns = ["Code Navision", "FN CEGID"]
            df_data["CHAMP TECHNIQUE"] = column["CHAMP TECHNIQUE"]
            merged_df = pd.merge(
                df_data,
                mapping_data[right_on_columns + ["Code cegid"]],
                how="left",
                indicator=True,
                left_on=left_on_columns,
                right_on=right_on_columns
            )

            merged_df = merged_df.replace(".", np.nan)

            merged_df.rename(
                columns={"Code cegid": column["champ Navision"]},
                inplace=True,
                errors="raise"
            )
        elif column["Mapping Sheet Name"] == "Num dim":
            df_data[column["champ Navision"]] = df_data[column["champ Navision"]].str.split().str[0]
            df_data[column["champ Navision"]] = df_data[column["champ Navision"]].str.capitalize()
            left_on_columns = [column["champ Navision"]]
            right_on_columns = ["Code Navision"]
            merged_df = pd.merge(
                df_data,
                mapping_data,
                how="left",
                indicator=True,
                left_on=left_on_columns,
                right_on=right_on_columns
            )

            merged_df.rename(
                columns={"Code cegid": column["champ Navision"]},
                inplace=True,
                errors="raise"
            )
        elif (column["Mapping Sheet Name"] == "Dimension") and (MAPPING_BLOB_NAME == "mapping/Structure Dimensions.csv"):
            left_on_columns = [column["champ Navision"]]
            right_on_columns = ["Code Navision"]
            merged_df = pd.merge(
                df_data,
                mapping_data,
                how="left",
                indicator=True,
                left_on=left_on_columns,
                right_on=right_on_columns
            )
            merged_df.rename(
                columns={"_merge": 'Verification'},
                inplace=True,
                errors="raise"
            )
            return merged_df
        elif MAPPING_BLOB_NAME in MOUVEMENT_STOCK_BLOB_NAMES: 

            left_on_columns = [column["champ Navision"]] #[column["NOM CHAMP"]]
            right_on_columns = ["Code cegid"]
            # Merge the data frames
        
            merged_df = pd.merge(
                df_data,
                mapping_data[right_on_columns + ["Code Navision"]],
                how="left",
                indicator=True,
                left_on=left_on_columns,
                right_on=right_on_columns
            )
            # Replace "." with np.nan
            merged_df = merged_df.replace(".", np.nan)
            # Rename the columns
            merged_df.rename(
                columns={"Code cegid": column["champ Navision"]},
                inplace=True,
                errors="raise"
            )
            return merged_df
        elif MAPPING_BLOB_NAME in COMMON_MAPPING_BLOB_NAMES: 

            left_on_columns = [column["NOM CHAMP"]]
            right_on_columns = ["Code cegid"]
            # Merge the data frames

            merged_df = pd.merge(
                df_data,
                mapping_data[right_on_columns + ["Code Navision"]],
                how="left",
                indicator=True,
                left_on=left_on_columns,
                right_on=right_on_columns
            )
            # Replace "." with np.nan
            merged_df = merged_df.replace(".", np.nan)
            # Rename the columns
            merged_df.rename(
                columns={"Code Navision": column["NOM CHAMP"]},
                inplace=True,
                errors="raise"
            )
        else :  
            left_on_columns = [column["champ Navision"]]
            right_on_columns = ["Code Navision"]
            # Merge the data frames
            merged_df = pd.merge(
                df_data,
                mapping_data[right_on_columns + ["Code cegid"]],
                how="left",
                indicator=True,
                left_on=left_on_columns,
                right_on=right_on_columns
            )
            # Replace "." with np.nan
            merged_df = merged_df.replace(".", np.nan)
            # Rename the columns
            merged_df.rename(
                columns={"Code cegid": column["champ Navision"]},
                inplace=True,
                errors="raise"
            )
            return merged_df
        

