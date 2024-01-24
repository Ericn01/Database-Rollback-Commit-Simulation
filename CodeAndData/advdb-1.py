# Adv DB Winter 2024 - 1

import random

data_base = []  # Global binding for the Database contents
'''
transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
                ['id3', 'attribute3', 'value3']]
'''
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = [] # <-- You WILL populate this as you go. Keeps track of the changes made to the database. 
# Every entry in DB_Log will represent a transaction and contain data like the transaction ID, the data before the transaction, the data before the transaction, and whether the transaction has been commited.

def recovery_script(log:list):  #<--- Your CODE
    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")
    pass

def transaction_processing(): #<-- Your CODE
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''

    for transaction in transactions:
        # Retrieve the index of the transaction
        transaction_index = data_base.index(transaction[0])
        data_base[transaction_index] = transaction 
    print(data_base)

    

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
    return result

def main():
    ''' Main function --> Explaining the functionality, step by step.
    
    1. We retrieve the number of transactions to be processed, followed by retrieving the data from the 'database' CSV file. This data is loaded into a list/array object. 
    2. Enter a loop where every transaction is processed, one by one until a failure occurs. The failure condition is simulated by the is_there_a_failure() function call. 
    3. Looking closer at the loop, we see that for each transaction the following occurs:
        a) A message notifying the user abnout the transaction status (success or failure) is printed.
        b) A check for the failure condition, provided by the is_there_a_failure function occurs. If there is a failure, i.e the return value is 'True', the must_recover variable is set to true, followed by a recording of the index of the failed transaction.
        c) In the case that no failure occurs, a message stating the success of the transaction is printed to the console.
    4. Once the looping is completed, if there was a failure, we will now call the recovery_script function which will return the DB to its state before the transactions occured. If the transactions did succeed, we will instead print a message indicating this, followed by the new entries in the DB / CSV file. 
    '''
    number_of_transactions = len(transactions)
    # Testing --> Chekc the number of transactions
    print(number_of_transactions);
    must_recover = False
    data_base = read_file('CodeAndData\Employees_DB_ADV.csv')
    # Testing --> Check that read_file is working properly
    print(data_base)
    failure = is_there_a_failure()
    failing_transaction_index = None
    while not failure:
        # Process transaction
        for index in range(number_of_transactions):
            print(f"\nProcessing transaction No. {index+1}.")    
            transaction_processing()#<--- Your CODE (Call function transaction_processing)
            print("UPDATES have not been committed yet...\n")
            failure = is_there_a_failure()
            if failure:
                must_recover = True
                failing_transaction_index = index + 1
                print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                break
            else:
                print(f'Transaction No. {index+1} has been commited! Changes are permanent.')
                
    if must_recover:
        #Call your recovery script
        recovery_script(DB_Log) ### Call the recovery function to restore DB to sound state
    else:
        # All transactiones ended up well
        print("All transaction ended up well.")
        print("Updates to the database were committed!\n")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)
    
main()


