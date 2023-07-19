"""
Connects to SQLite DB, executes SELECT queries and creates a CSV file.
"""
import sqlite3
import pandas


def select_and_insert(cursor, query):
    """Executes SQL query and creates a list of lists from the results data set."""
    cursor.execute(query)

    # Fetch all the results as a list of tuples
    results = cursor.fetchall()

    # Convert the list of tuples to a list of lists
    list_of_lists = [list(row) for row in results]

    return list_of_lists


if __name__ == '__main__':

    # Connect to test.db
    print("INITIATING CONNECTION TO 'test.db'.")
    conn = sqlite3.connect('test.db')
    c = conn.cursor()
    print("SUCCESSFULLY CONNECTED TO DB.")

    # Create settlements loss CSV file
    settlements_loss_select_query = """
    SELECT t.customerID, sum(t.price) as sum_loss FROM settlements s 
    join transactions t on s.betDataID = t.betDataID
    where s.verdict like 'L'
    and t.customerID not like '123456789'
    group by t.customerID;
    """

    settlements_loss_data_list = select_and_insert(c, settlements_loss_select_query)
    df = pandas.DataFrame(settlements_loss_data_list, columns=['customerID', 'sum_withdraw'])
    df.to_csv('settlements_loss.csv', index=False)
    print("CREATED SETTLEMENTS LOSS CSV FILE.")

    # Create settlements win CSV file
    settlements_win_select_query = """
        SELECT t.customerID, sum(t.price) as sum_win FROM settlements s 
        join transactions t on s.betDataID = t.betDataID
        where s.verdict like 'W'
        and t.customerID not like '123456789'
        group by t.customerID;
        """

    settlements_win_data_list = select_and_insert(c, settlements_win_select_query)
    df = pandas.DataFrame(settlements_win_data_list, columns=['customerID', 'sum_win'])
    df.to_csv('settlements_win.csv', index=False)
    print("CREATED SETTLEMENTS WIN CSV FILE.")

    # Create settlements withdrawals CSV file
    settlements_withdrawal_select_query = """
            SELECT t.customerID, sum(w.amount) as sum_withdraw FROM withdrawals w 
            join transactions t on w.accountID = t.customerID
            and t.customerID not like '123456789'
            group by t.customerID;
            """

    settlements_withdrawal_data_list = select_and_insert(c, settlements_withdrawal_select_query)
    df = pandas.DataFrame(settlements_withdrawal_data_list, columns=['customerID', 'sum_withdrawal'])
    df.to_csv('settlements_withdrawal.csv', index=False)
    print("CREATED SETTLEMENTS WITHDRAWAL CSV FILE.")

    conn.commit()
    conn.close()
    print("CLOSED CONNECTION TO DB.")