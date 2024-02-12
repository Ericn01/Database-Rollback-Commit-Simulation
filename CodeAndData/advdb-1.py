# Adv DB Winter 2024 - 1
import csv # write data to CSV files
import random
from DB_Log import DBLog # Import the DBLog class, which will be the objects that populate our DB_Log list

data_base = []  # Global binding for the Database contents
'''
transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
                ['id3', 'attribute3', 'value3']]
'''
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]

# Every entry in DB_Log will represent a transaction and contain data like the transaction ID, the data before the transaction, the data before the transaction, and whether the transaction has been commited.
DB_Log = []  # <-- You WILL populate this as you go
DATABASE_FILE_PATH = 'Employees_DB_ADV.csv'
JOURNAL_FILE_PATH = 'Transaction_Journal.csv'
SECONDARY_MEMORY_FILE_PATH = 'Committed_Employees_DB_ADV.csv' # Using this instead of overwriting the original database file 


def write_to_secondary_memory():
    # Save committed data to a secondary memory system (CSV file)
    with open(SECONDARY_MEMORY_FILE_PATH, 'w', newline='') as database_file:
        csv_writer = csv.writer(database_file)
        csv_writer.writerows(data_base)

def recovery_script(log_entries: list):
    '''
    Restore the database to a stable and sound condition by processing the DB log.
    '''
    for log_entry in log_entries:
        attribute_index = get_attribute_index(log_entry['attribute'])
        status = log_entry['status'] # retrieve the status of the given transaction / log entry
        if status == 'not-executed':
            # If the transaction was not executed, there is nothing to do
            continue
        elif status == 'rolled-back':
            # If the transaction has a status of rolled back, undo the changes
            index = log_entry['transaction_id']
            transaction = get_database_entry(str(index))
            data_base[int(transaction[0])][attribute_index] = log_entry['old_value']
        elif status == 'committed':
            # If the transaction was committed, there is nothing to do
            continue
    write_to_secondary_memory() # Update the previous values.
    print("Recovery completed.")

def commit_transaction(index, attribute, attribute_index, old_value, new_value):
    # Save changes to the database
    data_base[index][attribute_index] = new_value

    commit_transaction_log_entry = DBLog(index, attribute, old_value, new_value, 'committed')
    # Save commit entry to the journal
    DB_Log.append(commit_transaction_log_entry)
    # Write the new information to secondary memory
    

# apply the rollback status to every transaction that has been processed
def apply_rollback(failed_transaction_index):
    new_DB_Log = []  # Create a new list to store the updated entries
    
    # This will add a new entry to every transaction that has been commited, indicating that 
    for log_entry in DB_Log:
        id, attr, old_val, new_val =  log_entry['transaction_id'], log_entry['attribute'], log_entry['old_value'], log_entry['new_value']
        rollback_entry = DBLog(id, attr, old_val, new_val, 'rolled-back')
        new_DB_Log.append(log_entry)  # Append the original entry
        new_DB_Log.append(rollback_entry)  # Append the rollback entry
    
    # Update the original DB_Log list with the new entries
    DB_Log.clear() 
    DB_Log.extend(new_DB_Log)

    # Retrieve the transaction that failed
    failed_transaction = transactions[failed_transaction_index]
    # Extract the data from the transaction
    index, attribute, new_value = failed_transaction
    # Find the database entry that matches our transaction ID and retrieve the previous value for the given attribute
    corresponding_db_entry = get_database_entry(index)
    attribute_index = get_attribute_index(attribute)
    old_value = corresponding_db_entry[attribute_index]

    # Save rollback entry to the journal
    rollback_current_entry = DBLog(index, attribute, old_value, new_value, 'rolled-back')
    DB_Log.append(rollback_current_entry)

# Finds the index of a given attribute using the DB header
def get_attribute_index(attribute):
    database_header = data_base[0]
    return database_header.index(attribute)

# Find the corresponding entry to the transaction
def get_database_entry(index):
    current_entry = None
    for entry in data_base:
        if entry[0] == index:
            current_entry = entry
    return current_entry

def transaction_processing(transaction):
    '''
        1. Process transaction in the transaction queue.
        2. Updates DB_Log accordingly
        3. This function does NOT commit the updates, just execute them
    '''
    index, attribute, new_value = transaction

    attribute_index = get_attribute_index(attribute) # Find the index of the attribute using the header in the data_base list

    current_entry = get_database_entry(index)
    # Save the current state before executing the transaction
    old_value = current_entry[attribute_index]
    # Commit the transaction 
    commit_transaction(int(index), attribute, attribute_index, old_value, new_value)



def read_file(file_name:str)->list:
    '''
    Read the contents of a CSV file line-by-line and return a list of lists
    '''
    data = []
    #
    # one line at-a-time reading file
    #
    with open(file_name, 'r') as reader:
    # Read and print the entire file line by line
        line = reader.readline()
        while line != '':  # The EOF char is an empty string
            line = line.strip().split(',')
            data.append(line)
            # get the next line
            line = reader.readline()
    size = len(data)
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the database, including one header.\n")
    return data

def is_there_a_failure()->bool:
    '''
        Simulates randomly a failure, returning True or False, accordingly
    '''
    failure_weights = [0.7, 0.3] # Probability weights for an output of 0 (no failure) and 1 (failure), respectively
    value = random.choices([0, 1], weights=failure_weights, k=1)[0]
    if value == 1:
        result = True
    else:
        result = False
    return result

"""
    Generate a report of changes between the previous and current versions of a database.

    Parameters:
    - previous_data (list): The old version of the database.
    - current_data (list): The new version of the database.

    Returns:
    - list: A list containing strings describing the changes in the format: 
            "[Row {row_index}. {attribute}: {old_value} --> {new_value}]"
            Empty list if no changes are detected.
"""
# Prints out the data in the DB_Log list
def print_db_log(DB_Log):
    print("\nDATABASE LOG:")
    print("--------------")
    for entry in DB_Log:
        print(entry)

def main():
    global data_base
    ''' Main function.
        1. We retrieve the number of transactions to be processed, followed by retrieving the data from the 'database' CSV file. This data is loaded into a list/array object. 
        2. Enter a loop where every transaction is processed, one by one until a failure occurs. The failure condition is simulated by the is_there_a_failure() function call. 
        3. Looking closer at the loop, we see that for each transaction the following occurs:
            a) A message notifying the user about the transaction status (success or failure) is printed.
            b) A check for the failure condition, provided by the is_there_a_failure function occurs. If there is a failure, i.e the return value is 'True', the must_recover variable is set to true, followed by a recording of the index of the failed transaction.
            c) In the case that no failure occurs, a message stating the success of the transaction is printed to the console.
        4. Once the looping is completed, if there was a failure, we will now call the recovery_script function which will return the DB to its state before the transactions occured. If the transactions did succeed, we will instead print a message indicating this, followed by the new entries in the DB / CSV file. 
    '''
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file(DATABASE_FILE_PATH)
    failure = is_there_a_failure()
    failing_transaction_index = None
    # Account for the cases where a failure occurs before any transaction processing occurs.
    if failure:
        print("A failure occured before any transactions could be processed...")
        for transaction in transactions: # add entries to the db_log to let the user know that the transaction did not go through
            id, attribute, new_value = transaction
            old_value = get_database_entry(id)[get_attribute_index(attribute)]
            log_entry = DBLog(id, attribute, old_value, new_value, 'not-executed')
            DB_Log.append(log_entry)
        return # Exit the main system
    while not failure:
        transactions_remaining = True
        for index in range(number_of_transactions):
            print(f"\nProcessing transaction No. {index + 1}.")
            current_transaction = transactions[index] # The transaction that we are currently processing
            transaction_processing(current_transaction) # Process the given transaction and persist to second memory + add to log
            print("UPDATES have not been committed yet...\n")
            failure = is_there_a_failure()
            if failure: 
                must_recover = True
                failing_transaction_index = index + 1
                print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                break
            else:
                print(f'Transaction No. {index+1} has been commited! Changes are permanent.')

        transactions_remaining = index < number_of_transactions - 1  # Check to see if there are more transactions to process
        if not transactions_remaining:
            break  # Break out of the while loop if no transactions are remaining      
    if must_recover:
        print("Rolling back changes and recovering...")
        apply_rollback(failing_transaction_index)
        recovery_script(DB_Log)
    else:
        print("All transactions ended up well.")
        print("Updates to the database were COMMITTED to Committed_Employees_DB_ADV.csv!")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)

main() # Call the main function to perform transaction processing
print_db_log(DB_Log) # Output the contents of the DB Log
    
