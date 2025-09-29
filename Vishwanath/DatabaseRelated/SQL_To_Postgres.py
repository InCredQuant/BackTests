import sqlite3
import psycopg2
import pandas as pd

def sqlite_to_postgres(sqlite_db_path, pg_host, pg_database, pg_user, pg_password, pg_port=5432):
    try:
        sqlite_conn = sqlite3.connect(sqlite_db_path)
        pg_conn = psycopg2.connect(
            host=pg_host,
            database=pg_database,
            user=pg_user,
            password=pg_password,
            port=pg_port)
        # print(pg_conn)
        pg_cursor = pg_conn.cursor()
        # print(pg_cursor)
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        # print(sqlite_cursor)
        tables = [table[0] for table in sqlite_cursor.fetchall()]
        for table_name in tables:
            if table_name == 'FutPriceData':
                print(f"Processing table: {table_name}")
                sqlite_cursor.execute(f"PRAGMA table_info({table_name})")
                columns = sqlite_cursor.fetchall()
                print(columns)
                column_defs = []
                for col in columns:
                    col_name = col[1]
                    col_type = col[2].upper()
                    if 'INT' in col_type:
                        pg_type = 'INTEGER'
                    elif 'CHAR' in col_type or 'TEXT' in col_type or 'CLOB' in col_type:
                        pg_type = 'TEXT'
                    elif 'REAL' in col_type or 'FLOA' in col_type or 'DOUB' in col_type:
                        pg_type = 'FLOAT'
                    elif 'BOOL' in col_type:
                        pg_type = 'BOOLEAN'
                    elif 'DATE' in col_type or 'TIME' in col_type:
                        pg_type = 'TIMESTAMP'
                    elif 'BLOB' in col_type:
                        pg_type = 'BYTEA'
                    else:
                        pg_type = 'TEXT'  # Default to TEXT
                    not_null = "NOT NULL" if col[3] else ""
                    # pk = "PRIMARY KEY" if col[5] else ""                         ## Change this back to default
                    pk = "" if col[5] else ""
                    column_defs.append(f"\"{col_name}\" {pg_type} {not_null} {pk}")
                create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(column_defs)});"
                print(create_table_sql)
                try:
                    pg_cursor.execute(create_table_sql)
                    pg_conn.commit()
                except Exception as e:
                    print(f"Error creating table {table_name}: {e}")
                    pg_conn.rollback()
                    continue
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
                print(df.columns)
                # if not df.empty:
                #     pg_cursor.execute(f"TRUNCATE TABLE \"{table_name}\";")
                #     batch_size = 10000
                #     for i in range(0, len(df), batch_size):
                #         batch_df = df.iloc[i:i+batch_size]
                #         columns_str = '", "'.join(batch_df.columns)
                #         values_list = []
                #         for _, row in batch_df.iterrows():
                #             placeholders = ", ".join(["%s"] * len(row))
                #             insert_query = f'INSERT INTO "{table_name}" ("{columns_str}") VALUES ({placeholders})'
                #             values = [None if pd.isna(x) else x for x in row]
                #             try:
                #                 pg_cursor.execute(insert_query, values)
                #             except Exception as e:
                #                 print(f"Error inserting row: {e}")
                #                 pg_conn.rollback()
                #         pg_conn.commit()
                    
                #     print(f"Table {table_name} cloned successfully with {len(df)} rows.")
            
            # Close connections
            # sqlite_conn.close()
            # pg_cursor.close()
            # pg_conn.close()
            
            print("Database cloning completed.")
        
    except Exception as e:
        print(f"Error during migration: {e}")

# Example usage
sqlite_db_path = rf"C:\Vishwanath\PythonCodes\Strategy\DatabaseRelated\PriceData.db"
pg_host = "192.168.44.4"  # or your PostgreSQL server address
pg_database = "pricedata"  # target PostgreSQL database name
pg_user = "postgres"  # PostgreSQL username
pg_password = "postgres"  # PostgreSQL password

# Call the function
sqlite_to_postgres(sqlite_db_path, pg_host, pg_database, pg_user, pg_password)