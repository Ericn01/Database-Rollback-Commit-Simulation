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
DB_Log = [] # <-- You WILL populate this as you go. Keeps track of the changes made to the database. 
# Every entry in DB_Log will represent a transaction and contain data like the transaction ID, the data before the transaction, the data before the transaction, and whether the transaction has been commited.


def recovery_script(log:list):  
    '''
        Rollback changes based on the provided DBLog objects
    '''
    for log_entry in reversed(log):
        transaction_id, attribute, old_value = log_entry.transaction_id, log_entry.attribute, log_entry.old_value
        transaction_index = data_base.index(transaction_id)
        data_base[transaction_index][attribute] = old_value

def transaction_processing(error_occurred, DB_Log, data_base):
    while (not error_occurred):
        for transaction in transactions:
            transaction_id, attribute, new_value = transaction[0], transaction[1], transaction[2]
            print(transaction_id)
            # Find the index of the transaction_id in the first column of data_base
            transaction_index = None
            for index, row in enumerate(data_base):
                print(index, row)
                if row[0] == transaction_id:
                    transaction_index = index
                    break

            if transaction_index is not None:
                old_value = data_base[transaction_index][attribute]
                log_entry = DBLog(transaction_id, table_name='Employee', attribute=attribute, old_value=old_value, new_value=new_value)
                DB_Log.append(log_entry)
                data_base[transaction_index][attribute] = new_value
            else:
                print(f"Warning: Transaction ID '{transaction_id}' not found in the database.")


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
    value = random.randint(0,1)
    if value == 1:
        result = True
    else:
        result = False
    return 0 # Change: no failures for now

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
def commit_changes(data, new_file_name):
    with open(new_file_name, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

def generate_change_report(original_file, commit_file):
    report = []
    # Assuming transactions contain attribute indices
    attribute_indices = [int(transaction[1]) for transaction in transactions]

    with open(original_file, 'r', newline='') as original, open(commit_file, 'r', newline='') as new:
        original_reader = csv.reader(original)
        new_reader = csv.reader(new)

        for row_index, (original_row, new_row) in enumerate(zip(original_reader, new_reader)):
            for col_index, (original_value, new_value) in enumerate(zip(original_row, new_row)):
                if col_index < len(attribute_indices):  # Check if col_index is within attribute_indices length
                    attribute_index = attribute_indices[col_index]
                    attribute = f"Attribute {attribute_index}"
                    if original_value != new_value:
                        change_entry = f"[Row {row_index + 1}. {attribute}: {original_value} --> {new_value}]"
                        report.append(change_entry)

    return report

def main():
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
    data_base = read_file('Employees_DB_ADV.csv')
    failure = is_there_a_failure()
    failing_transaction_index = None
    while not failure:
        for index in range(number_of_transactions):
            print(f"\nProcessing transaction No. {index + 1}.")
            transaction_processing(failure, DB_Log, data_base)
            print("UPDATES have not been committed yet...\n")
            failure = is_there_a_failure()
            if failure:
                must_recover = True
                failing_transaction_index = index + 1
                print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                break
            else:
                print(f'Transaction No. {index + 1} has been committed! Changes are permanent.')
    if must_recover:
        print('')
        recovery_script(DB_Log)
    else:
        print("All transactions ended up well.")
        # Commit changes to a new file
        new_file_name = 'Committed_Employees_DB_ADV.csv'
        commit_changes(data_base, new_file_name)
        print(f"Updates to the database were COMMITTED to {new_file_name}!\n")

        # Generate and print the comparison report
        #change_report = generate_change_report('Employees_DB_ADV.csv', new_file_name)
        change_report = None
        if change_report:
            print("DATABASE COMPARISON REPORT:")
            print("----------------------------")
            for entry in change_report:
                print(entry)
        else:
            print("No changes detected.")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)
    
    print(DB_Log)


main()