# Import required libraries
from neo4j import GraphDatabase
import pandas as pd
import psycopg2

# Define Neo4j connection details
neo4j_uri = "neo4j+s://24cd4ce5.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "zJ_eb2X2orrPbRW4KMZ7HloSPoRqMmm-20cJS743Z8o"

# Define Postgres connection details
pg_host = "database-1.c4d8dly4wino.ap-south-1.rds.amazonaws.com"
pg_database = "telecom_data"
pg_user = "postgres"
pg_password = "1S9RIOSXWqxQ7pxGzDlt"

# Define Neo4j query to extract data
neo4j_query = """
    MATCH (c:Customer)-[s:SUBSCRIBES_TO]->(svc:Service)
    RETURN c.customer_id, s.subscription_id, svc.service_id,s.start_date, s.end_date, s.price
"""
# Define function to extract data from Neo4j and return a Pandas DataFrame
def extract_data():
# Connect to Neo4j
driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
with driver.session() as session:
        result = session.run(neo4j_query) 
        df = pd.DataFrame([r.values() for r in result], columns=result.keys())
        return df

# Define function to transform data
def transform_data(df):
    # Convert date fields to datetime objects
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
        # Remove null values
    df.dropna(inplace=True)   
    return df

# Define function to load data into Postgres
def load_data(df):
    # Connect to Postgres
    conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)
    # Create table if it doesn't exist
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS telecom_data (
            customer_id INTEGER,
            subscription_id INTEGER,
            service_id INTEGER,
            start_date DATE,
            end_date DATE,
            price FLOAT
        )
        """)
    # Insert data into table
    
    conn.commit()
    conn.close()

# Define main function
def main():
    data = extract_data()    
    df = transform_data(data)    
    load_data(df)
    

# Call main function
if __name__ == "__main__":
    main()
