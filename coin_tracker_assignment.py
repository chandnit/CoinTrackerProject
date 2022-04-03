import sqlite3
import requests
from datetime import datetime


#create a database connection using sqllite
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except:
        print('connection failed')

    return conn

#create a table using the db conn
def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except:
        print('create table failed')

#create 3 tables: user, address, and transactions
def create_user_address_transaction_tables(conn):
    sql_create_users_table = """ CREATE TABLE IF NOT EXISTS users (
                                                id integer PRIMARY KEY,
                                                username text NOT NULL,
                                                first_name text,
                                                last_name text,
                                                email text
                                            ); """

    sql_create_addresses_table = """ CREATE TABLE IF NOT EXISTS addresses (
                                                id integer PRIMARY KEY,
                                                address text NOT NULL,
                                                user_id text NOT NULL,
                                                balance text,
                                                last_synced_time datetime NOT NULL
                                            ); """

    sql_create_transactions_table = """ CREATE TABLE IF NOT EXISTS transactions (
                                                id integer PRIMARY KEY,
                                                transaction_hash text,
                                                from_address text NOT NULL,
                                                to_address text NOT NULL,
                                                transaction_time datetime NOT NULL
                                            ); """

    # create tables
    if conn is not None:
        create_table(conn, sql_create_users_table)
        create_table(conn, sql_create_addresses_table)
        create_table(conn, sql_create_transactions_table)
    else:
        print("Cannot connect to db")

#insert user to db
def add_user(conn, user_info):
    sql = ''' INSERT INTO users(username,first_name,last_name,email)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, user_info)
    conn.commit()
    return cur.lastrowid

#insert address to db
def add_address(conn, address_info):
    sql = ''' INSERT INTO addresses(address,user_id,balance,last_synced_time)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, address_info)
    conn.commit()
    return cur.lastrowid

#delete address from db
def delete_address_db(conn, address):
    sql = ''' DELETE FROM addresses WHERE address = ?'''
    cur = conn.cursor()
    cur.execute(sql, (address,))
    conn.commit()

#print statement for checking results
def print_address_table(conn):
    sql = '''SELECT * FROM addresses'''
    cur = conn.cursor()
    cur.execute(sql)
    print(cur.fetchall())

#insert transaction to db
def add_transaction(conn, transaction_info):
    sql = ''' INSERT INTO transactions(transaction_hash,from_address,to_address, transaction_time)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, transaction_info)
    conn.commit()
    return cur.lastrowid

#update value of balance and last_synced time in db given the address
def update_balance(conn, address, balance, last_synced_time):
    sql = ''' UPDATE addresses
                  SET address = ? ,
                      balance = ? ,
                      last_synced_time = ?
                  WHERE address = ?1'''
    cur = conn.cursor()
    cur.execute(sql, (address, balance, last_synced_time))
    conn.commit()

#returns transactions, either from or two that match the given address provided
def get_current_transactions(conn, address):
    sql = '''SELECT * FROM transactions WHERE from_address=? or to_address=?'''
    cur = conn.cursor()
    cur.execute(sql, (address,address))

    rows = cur.fetchall()
    return rows

#calls the api to fetch the latest balance
def retrieve_latest_balance(address):
    url = f'https://api.blockchair.com/bitcoin/dashboards/address/{address}'
    response = requests.get(url)
    data = response.json()['data']
    balance = data[address]['address']['balance']
    return balance

#calls the api to fetch the lastest transactions
def retrieve_latest_transactions(address):
    #limited to 5 transactions for demo purposes
    url = f'https://api.blockchair.com/bitcoin/dashboards/address/{address}?limit=5'
    response = requests.get(url)
    data = response.json()['data']
    transaction_hashes = data[address]['transactions']
    return transaction_hashes

#updates the current transactions in the database with unique transactions returned from the api
def update_transaction_list(local_transactions, api_transactions):
    updated_transactions = []
    for transaction in api_transactions:
        if transaction not in local_transactions:
            updated_transactions.append(transaction)
    return list(updated_transactions)

#returns details about the transaction given the transaction hash
def retrieve_transactions_details(conn, address):
    api_transactions = retrieve_latest_transactions(address)
    local_transactions = get_current_transactions(conn,address)

    transactions = update_transaction_list(local_transactions, api_transactions)

    transactions_details = []
    for transaction_hash in transactions:
        url = f'https://api.blockchair.com/bitcoin/dashboards/transaction/{transaction_hash}'
        data = requests.get(url).json()['data']
        from_address = data[transaction_hash]['inputs'][0]['recipient']
        to_address = data[transaction_hash]['outputs'][0]['recipient']
        transaction_time = data[transaction_hash]['transaction']['time']
        transactions_details.append((transaction_hash, from_address, to_address, transaction_time))
    return transactions_details

#gets all the rows from the transaction table
def get_all_transactions(conn):
    sql = '''SELECT * FROM transactions'''
    cur = conn.cursor()
    cur.execute(sql)
    transactions = cur.fetchall()
    return transactions;

#gets all the addresses from the address table
def get_all_addresses(conn):
    sql = '''SELECT * FROM addresses'''
    cur = conn.cursor()
    cur.execute(sql)
    addresses = cur.fetchall()
    return addresses;

def main():
    #create the db in memory
    conn = create_connection(':memory:')

    #create table structure
    create_user_address_transaction_tables(conn)

    #create user and insert into db
    user_cade_info =  ('motorcade', 'Cade', 'Cunningham', 'ccunningham@gmail.com')
    user_cade_id = add_user(conn, user_cade_info)


    #create two bitcoin test addresses for our user cade
    address_for_cade_1 = '12xQ9k5ousS8MqNsMBqHKtjAtCuKezm2Ju'
    address_info_1 = (address_for_cade_1, user_cade_id, '100000', datetime.now())

    address_for_cade_2 = 'bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h'
    address_info_2 = (address_for_cade_2, user_cade_id, '256000', datetime.now())

    #Requirement #1 - Insert address into db
    address_id_1 = add_address(conn, address_info_1)
    address_id_2 = add_address(conn, address_info_2)

    #print addresses table to verify values were inserted correctly
    print("Address Table after inserting new addresses - Requirement #1")
    print_address_table(conn)
    print('\n')

    #Requirement #1- Delete address from db
    delete_address_db(conn, address_for_cade_1)

    #print addresses table to verify address was deleted from db
    print("Address Table after removing first address- Requirement #1")
    print_address_table(conn)
    print('\n')

    #Requirement #1 reinsert first address
    address_id_3 = add_address(conn, address_info_1)
    print("Address Table after reinserting first address- Requirement #1")
    print_address_table(conn)
    print('\n')

    #getting all addresses in db
    addresses =  get_all_addresses(conn)

    #Requirement #2, #3 Synchronize bitcoin wallet transactions for the addresses and 
    #retrieve the current balances and transactions for each bitcoin address. 
    #have limited it to 5 responses per address so that it finishes in reasonable amount of time
    for address in addresses:
        latest_balance = retrieve_latest_balance(address[1])
        if(latest_balance != address[3]):
            update_balance(conn, address[1], latest_balance, datetime.now())
        get_transactions = retrieve_transactions_details(conn, address[1])
        for transactions_details in get_transactions:
         add_transaction(conn, transactions_details)

    #since we deleted and reinserted the user '12xQ9k5ousS8MqNsMBqHKtjAtCuKezm2Ju', it will be at the end of the addresses. Another thing to note is the balance has been updated from the call to the API
    print("Address Table With Updated Balance After API call - Requirement #2, #3")
    print_address_table(conn)
    print('\n')

    #print the transactions after the call the to API, this will sync the transactions to the transactions table. I have limited it to 5 per address due to the amount of data
    print("5 Transactions Per Address in Address Table - Requirement #2, #3")
    get_transactions = get_all_transactions(conn)
    print(get_transactions)

    conn.close()

if __name__ == '__main__':
    main()