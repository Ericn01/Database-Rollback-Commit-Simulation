from datetime import datetime # imported to add timestamps

# Implementing the object for the database log
class DBLog:
    def __init__(self, transaction_id, table_name, attribute, old_value, new_value):
        self.transaction_id = transaction_id
        self.table_name = table_name
        self.attribute = attribute
        self.old_value = old_value
        self.new_value = new_value
        self.timestamp = datetime.now()

    def __str__(self):
        return f"Timestamp: {self.timestamp}, Transaction ID: {self.transaction_id}, Table: {self.table_name}, Attribute: {self.attribute}, Old Value: {self.old_value}, New Value: {self.new_value}"


