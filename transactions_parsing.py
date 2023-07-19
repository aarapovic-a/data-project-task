""" protoc URL: https://github.com/protocolbuffers/protobuf/releases/tag/v23.4 """
import transaction_pb2
from google.protobuf.json_format import MessageToDict
import sqlite3


def deserialize_transactions(file_path):
    """
    Reads and deserializes Protobuf records from the binary transactions file.
    Parameters:
        file_path (str): The path to the binary transactions file.
    Yields:
        The next deserialized Protobuf record.
    """
    with open(file_path, 'rb') as binary_file:
        while True:
            # Read the length byte
            length_byte = binary_file.read(1)

            # If no data is read, we've reached the end of the file
            if not length_byte:
                break

            # Read the message using the length_byte to determine the message size
            message_size = ord(length_byte)
            message_data = binary_file.read(message_size)

            # Deserialize the Protobuf record from the message_data
            transaction = transaction_pb2.Transaction()
            try:
                transaction.ParseFromString(message_data)
                # dict_request = MessageToDict(transaction)
                # print(dict_request)
                yield transaction
            except Exception as e:
                # Handle any exceptions due to potential glitches
                print(f"Error parsing the Protobuf message: {e}")


def ensure_keys_and_values(dictionary):
    """ Sets default value (123456789) to transactions dictionary keys if value is not found for the given key. """
    # Define a list of the keys to check if value is not present
    required_keys = ['BETDATAID', 'TIMESTAMP', 'CUSTOMERID', 'OUTCOMEID', 'TRANSACTIONID', 'PRICE',
                     'UNITSTAKE', 'EVENTID', 'EVENTDESCRIPTION', 'MARKETDESCRIPTION', 'OUTCOMEDESCRIPTION', 'SUBTYPEDESCRIPTION']

    # Loop through the required keys and set 123456789 as the default value
    for key in required_keys:
        dictionary.setdefault(key, 123456789)

    return dictionary


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # Connect to test.db
    print("INITIATING CONNECTION TO 'test.db'.")
    conn = sqlite3.connect('test.db')
    c = conn.cursor()
    print("SUCCESSFULLY CONNECTED TO DB.")

    file_path = 'transactions.bin'

    transactions_file_contents = list()
    # Iterate over transactions
    for transaction in deserialize_transactions(file_path):
        # Process each transaction as needed
        dict_request = MessageToDict(transaction)

        # Ensure that the file contents list of dictionary has all the necessary keys
        modified_dict = ensure_keys_and_values(dict_request)

        transactions_file_contents.append(modified_dict)
        print(modified_dict)

    # Populate transactions table
    insert_sql = """
        INSERT INTO transactions (betDataID, timestamp, customerID, outcomeID, transactionID, price, unitstake, eventID, eventDescription, marketDescription, outcomeDescription, subtypeDescription)
        VALUES (:BETDATAID, :TIMESTAMP, :CUSTOMERID, :OUTCOMEID, :TRANSACTIONID, :PRICE, :UNITSTAKE, :EVENTID, :EVENTDESCRIPTION, :MARKETDESCRIPTION, :OUTCOMEDESCRIPTION, :SUBTYPEDESCRIPTION)
        """

    print("INSERTING DATA INTO TRANSACTIONS TABLE.")
    for data in transactions_file_contents:
        try:
            c.execute(insert_sql, data)
            print(f"DATA INSERTED: {data}")
        except sqlite3.IntegrityError as error:
            # Avoid inserting duplicates
            print(error)
            print(f"AVOID DATA INSERTED FOR: {data}")
            pass

    conn.commit()
    conn.close()
    print("CLOSED CONNECTION TO DB.")