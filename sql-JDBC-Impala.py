import jaydebeapi
import os

# --- Configuration ---

# Set JAVA_HOME if it's not configured in your environment
# This points to the main directory of your Java installation
os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk'

# Path to your downloaded Impala JDBC driver JAR file
impala_driver_jar = "/Users/dennislee/workspace/ImpalaJDBC42-2.6.33.1062.jar"

# --- IMPORTANT ---
# Added ';AllowSelfSignedCerts=1' to the end of the URL to disable SSL certificate validation.
jdbc_url = "jdbc:impala://coordinator-impala1.apps.dlee5.cldr.example:443/default;AuthMech=3;transportMode=http;httpPath=cliservice;ssl=1;AllowSelfSignedCerts=1"

# The jaydebeapi library takes credentials separately
credentials = ["dennis", "PASSWORD"] 
sql_query = "SELECT count(*) FROM db1.celltowers"

# --- Connection and Query Execution ---
conn = None
try:
    # Establish the connection
    print("Attempting to connect to Impala...")
    conn = jaydebeapi.connect(
        "com.cloudera.impala.jdbc.Driver", # The main driver class name
        jdbc_url,
        credentials,
        impala_driver_jar
    )
    print("Successfully connected to Impala.")
    
    # Create a cursor and execute the query
    curs = conn.cursor()
    curs.execute(sql_query)
    
    # Fetch the result
    result = curs.fetchone()
    if result:
        print("\nQuery Result:")
        print("------------")
        print(f"COUNT(*): {result[0]}")
        print("------------")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if conn:
        conn.close()
        print("Connection closed.")

