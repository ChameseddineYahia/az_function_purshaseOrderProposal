from marshmallow.exceptions import ValidationError
import logging

class SchemaValidation:
    def __init__(self, schema):
        self.schema = schema
        self.rows = []

    def handle_required_errors(self, data= {}):
        valid_rows = []
        invalid_rows = []

        for idx, item in enumerate(data["rows"]):
            try:
                self.schema.load(item)
                valid_rows.append(item)
            except ValidationError as err:
                missing_required_fields = {key: value for key, value in err.messages_dict.items()
                                           if any('required' in v for v in value) or any('Field may not be null' in v for v in value)}

                if missing_required_fields:
                    invalid_rows.append({**item, **{"Status": f"Not Inserted {missing_required_fields}"}})
                    # logging.error('handle type invalid rows: ' + str(item) + ', {"Status": "Not Inserted ' + str(missing_required_fields) + '"}')

                else:
                    valid_rows.append(item)
                    # logging.info(f"handle required valid lines {item}")
                
        self.rows = valid_rows
        return {"valid_rows": valid_rows, "invalid_rows": invalid_rows}

    def handle_type_errors(self, data = {}):
        all_rows = []
        valid_rows = []
        invalid_rows = []

        for idx, item in enumerate(self.rows):
            try:
                self.schema.load(item)
                valid_rows.append(item)
                all_rows.append(item)
            except ValidationError as err:
                non_required_fields = {key: value for key, value in err.messages_dict.items()
                                       if not any('required' in v for v in value)}

                if non_required_fields:
                    invalid_rows.append({**item, **{"Status": f"  {non_required_fields}"}})
                    all_rows.append(item)
                    # logging.error('handle type invalid rows: ' + str(item) + ', {"Status": "Not Inserted ' + str(non_required_fields) + '"}')

                else:
                    valid_rows.append(item)
                    all_rows.append(item)
            except Exception as e:
                all_rows.append(item)
               
                

        return {"valid_rows": valid_rows, "invalid_rows": invalid_rows , "all_rows" : all_rows}