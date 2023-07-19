import json
import sqlite3


def extract_settlements_withdrawals(file_path):
    """ Extracting withdrawals and settlements from .txt file and storing them into separate lists. """
    settlements = []
    withdrawals = []

    with open(file_path, 'r') as file:
        for line in file:
            data = json.loads(line.strip())
            event_type = data.get('eventType')

            if event_type == 'SETTLEMENT':
                settlements.append(data)
            elif event_type == 'WITHDRAWAL':
                withdrawals.append(data)

    return settlements, withdrawals


if __name__ == '__main__':

    # Connect to test.db
    print("INITIATING CONNECTION TO 'test.db'.")
    conn = sqlite3.connect('test.db')
    c = conn.cursor()
    print("SUCCESSFULLY CONNECTED TO DB.")

    # Parse .txt file
    file_path = 'settlementswithdrawals.txt'
    settlements, withdrawals = extract_settlements_withdrawals(file_path)

    # Populate settlements table
    settlements_insert_sql = """
                INSERT INTO settlements (betDataID, eventType, transactionID, verdict) 
                VALUES (:betDataId, :eventType, :transactionId, :verdict)
                """

    print("INSERTING SETTLEMENTS INTO DB:")
    for settlement in settlements:
        print(settlement)
        c.execute(settlements_insert_sql, settlement)
    conn.commit()

    # Populate withdrawals table
    withdrawals_insert_sql = """
            INSERT INTO withdrawals (accountID, eventType, amount) VALUES (:accountId, :eventType, :amount)
            """

    print("INSERTING WITHDRAWALS INTO DB:")
    for withdrawal in withdrawals:
        print(withdrawal)
        try:
            c.execute(withdrawals_insert_sql, withdrawal)
        except sqlite3.IntegrityError:
            # Avoid inserting duplicates
            pass
    conn.commit()

    # Close connection to DB
    conn.commit()
    conn.close()
    print("CLOSED CONNECTION TO DB.")


