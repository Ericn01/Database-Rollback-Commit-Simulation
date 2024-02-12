from datetime import datetime # imported to add timestamps

# Implementing the object for the database log
class DBLog:
    def __init__(self, transaction_id, attribute, old_value, new_value, status):
        self.transaction_id = transaction_id
        self.attribute = attribute
        self.old_value = old_value
        self.new_value = new_value
        self.status = status
        self.timestamp = datetime.now()

    def __str__(self):
        return f"Timestamp: {self.timestamp}, Transaction ID: {self.transaction_id}, Attribute: {self.attribute}, Old Value: {self.old_value}, New Value: {self.new_value}, Transaction Status: {self.status}"
    
    def __getitem__(self, key):
        return getattr(self, key, None)


