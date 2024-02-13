import csv 

def write_to_secondary_memory(SECONDARY_MEMORY_FILE_PATH, data_base):
    # Save committed data to a secondary memory system (CSV file)
    print("")
    with open(SECONDARY_MEMORY_FILE_PATH, 'w', newline='') as database_file:
        csv_writer = csv.writer(database_file)
        csv_writer.writerows(data_base)

def write_db_log_to_csv(JOURNAL_FILE_PATH, DB_Log):
    with open(JOURNAL_FILE_PATH, 'a', newline='') as log_file:
        log_writer = csv.writer(log_file, delimiter='\t')

        # Find the next available log number
        log_number = 1
        while f'DATABASE LOG ({log_number})' in open(JOURNAL_FILE_PATH).read():
            log_number += 1

        # Write the new log block to the file
        log_file.write(f"DATABASE LOG ({log_number})\n")
        log_file.write("-------------------------\n")

        for log_entry in DB_Log:
            log_entry_as_list = str(log_entry).split('\t')
            log_writer.writerow(log_entry_as_list)

        log_file.write("-------------------------\n\n")

# Find the corresponding entry to the transaction
def get_database_entry(index, data_base):
    current_entry = None
    for entry in data_base:
        if entry[0] == index:
            current_entry = entry
    return current_entry

# Prints out the data in the DB_Log list
def print_db_log(DB_Log):
    print("\nDATABASE LOG:")
    print("--------------")
    for entry in DB_Log:
        print(entry)

# Finds the index of a given attribute using the DB header
def get_attribute_index(attribute, data_base):
    database_header = data_base[0]
    return database_header.index(attribute)
        