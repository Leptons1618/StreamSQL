import pyodbc

# Database connection details
server = '192.168.1.87'
database = 'TestCDC'
username = 'sa'
password = 'Axcend123'
driver = '{ODBC Driver 17 for SQL Server}'  # Update if you use a different driver

# Connection string
conn_str = f"""
DRIVER={driver};
SERVER={server};
DATABASE={database};
UID={username};
PWD={password};
"""

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("✅ Connection successful.")

    # # INSERT operation
    # insert_data = [
    #     # ('Anish', 'G', 'anish.g@example.com'),
    #     # ('Soubhagya', 'P', 'soubhagya.p@example.com'),
    #     # ('Sabiha', 'S', 'sabiha.s@example.com'),
    #     # ('Saumik', 'K', 'saumik.k@example.com'),
    #     ('Srujan', 'S', 'srujan.s@example.com'),
    #     # ('Saiful', 'I', 'saiful.i@example.com')
    # ]

    # insert_query = """
    # INSERT INTO dbo.Customers (FirstName, LastName, Email)
    # VALUES (?, ?, ?)
    # """

    # cursor.executemany(insert_query, insert_data)
    # conn.commit()
    # print("✅ Insert operation complete.")

    # UPDATE operation
    update_query = """
    UPDATE dbo.Customers
    SET Email = ?
    WHERE FirstName = ? AND LastName = ?
    """

    update_data = [
        # ('soubhagya.updated@example.com', 'Soubhagya', 'P'),
        # ('sabiha.updated@example.com', 'Sabiha', 'S'),
        # ('saumik.updated@example.com', 'Saumik', 'K'),
        ('srujan.updated@example.com', 'Srujan', 'S'),
        # ('saiful.updated@example.com', 'Saiful', 'I')
    ]

    cursor.executemany(update_query, update_data)
    conn.commit()
    print("✅ Update operation complete.")

except pyodbc.Error as e:
    print("❌ Error:", e)

finally:
    if 'conn' in locals():
        conn.close()
        print("✅ Connection closed.")