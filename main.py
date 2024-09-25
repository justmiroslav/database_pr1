import os, mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    "host": os.getenv("host"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "database": os.getenv("database")
}

def create_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
    return None

def execute_query(connection, query, fetch=False):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        if fetch:
            return cursor.fetchone()
    except Error as e:
        print(f"Error executing query: {e}")
    finally:
        cursor.close()

def read_uncommitted():
    connection1, connection2 = create_connection(), create_connection()
    try:
        connection1.start_transaction(isolation_level="READ UNCOMMITTED")
        connection2.start_transaction(isolation_level="READ UNCOMMITTED")

        execute_query(connection1, "UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")
        balance_dirty_read = execute_query(connection2, "SELECT balance FROM accounts WHERE name = 'Alice'", fetch=True)[0]
        print(f"Dirty Read (READ UNCOMMITTED): Alice's balance = {balance_dirty_read}")
        connection1.rollback()
        connection2.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        connection1.close()
        connection2.close()

def read_committed():
    connection1, connection2 = create_connection(), create_connection()
    try:
        connection1.start_transaction(isolation_level="READ COMMITTED")
        connection2.start_transaction(isolation_level="READ COMMITTED")

        execute_query(connection1, "UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")
        balance_before_commit = execute_query(connection2, "SELECT balance FROM accounts WHERE name = 'Alice'", fetch=True)[0]
        print(f"Before commit (READ COMMITTED): Alice's balance = {balance_before_commit}")

        connection1.commit()
        balance_after_commit = execute_query(connection2, "SELECT balance FROM accounts WHERE name = 'Alice'", fetch=True)[0]
        print(f"After commit (READ COMMITTED): Alice's balance = {balance_after_commit}")

        connection2.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        connection1.close()
        connection2.close()

def repeatable_read():
    connection1, connection2 = create_connection(), create_connection()
    try:
        connection1.start_transaction(isolation_level="REPEATABLE READ")
        connection2.start_transaction(isolation_level="REPEATABLE READ")

        balance1 = execute_query(connection1, "SELECT balance FROM accounts WHERE name = 'Bob'", fetch=True)[0]
        print(f"First read (REPEATABLE READ): Bob's balance = {balance1}")

        execute_query(connection2, "UPDATE accounts SET balance = balance + 500 WHERE name = 'Bob'")
        connection2.commit()

        balance2 = execute_query(connection1, "SELECT balance FROM accounts WHERE name = 'Bob'", fetch=True)[0]
        print(f"Second read (REPEATABLE READ): Bob's balance = {balance2}")

        connection1.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        connection1.close()
        connection2.close()

def non_repeatable_read():
    connection1, connection2 = create_connection(), create_connection()
    try:
        connection1.start_transaction(isolation_level="READ COMMITTED")
        connection2.start_transaction(isolation_level="READ COMMITTED")

        balance1 = execute_query(connection1, "SELECT balance FROM accounts WHERE name = 'Bob'", fetch=True)[0]
        print(f"First read (READ COMMITTED): Bob's balance = {balance1}")

        execute_query(connection2, "UPDATE accounts SET balance = balance + 200 WHERE name = 'Bob'")
        connection2.commit()

        balance2 = execute_query(connection1, "SELECT balance FROM accounts WHERE name = 'Bob'", fetch=True)[0]
        print(f"Second read (READ COMMITTED): Bob's balance = {balance2}")

        connection1.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        connection1.close()
        connection2.close()

def deadlock():
    connection1, connection2 = create_connection(), create_connection()
    try:
        connection1.start_transaction()
        connection2.start_transaction()

        execute_query(connection1, "UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice'")
        execute_query(connection2, "UPDATE accounts SET balance = balance + 100 WHERE name = 'Bob'")

        connection1.commit()
        connection2.commit()
    except Error as e:
        print(f"Deadlock detected: {e}")
        connection1.rollback()
        connection2.rollback()
    finally:
        connection1.close()
        connection2.close()

def main():
    print("1. READ UNCOMMITTED (Dirty Read)")
    read_uncommitted()
    print("\n2. READ COMMITTED")
    read_committed()
    print("\n3. REPEATABLE READ")
    repeatable_read()
    print("\n4. Non-repeatable Read")
    non_repeatable_read()
    print("\n5. Deadlock")
    deadlock()

if __name__ == "__main__":
    main()
