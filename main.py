# main.py

import argparse
import json
import logging
import os
import re # Import regex for parsing SQL*Plus string

# Import updated modules
from db_connector import DatabaseConnection
from query_builder import QueryBuilder
from data_processor import DataProcessor
from config import DB_USER, DB_PASS, DB_DSN, LOG_LEVEL # Import original config values as fallback

# Configure logging for the main script
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

def load_query_config(file_path: str) -> dict:
    """
    Loads the query configuration from a JSON file.

    Args:
        file_path (str): The path to the JSON configuration file.

    Returns:
        dict: The loaded JSON content as a dictionary.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        json.JSONDecodeError: If the file content is not valid JSON.
    """
    if not os.path.exists(file_path):
        logging.error(f"Error: JSON file not found at '{file_path}'")
        raise FileNotFoundError(f"JSON file not found at '{file_path}'")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            logging.info(f"Successfully loaded query configuration from '{file_path}'.")
            return config
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from '{file_path}': {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading JSON: {e}")
        raise

def parse_sqlplus_connection_string(conn_str: str) -> tuple[str, str, str]:
    """
    Parses a SQL*Plus-style connection string (e.g., "user/pass@dsn")
    into username, password, and DSN components.

    Args:
        conn_str (str): The SQL*Plus connection string.

    Returns:
        tuple[str, str, str]: A tuple containing (username, password, dsn).

    Raises:
        ValueError: If the connection string format is invalid.
    """
    # Regex to match user/pass@dsn format
    # Group 1: username, Group 2: password, Group 3: dsn
    match = re.match(r'^([^/]+)/([^@]+)@(.+)$', conn_str)
    if not match:
        raise ValueError(
            f"Invalid SQL*Plus connection string format. Expected 'user/pass@dsn'. Got: '{conn_str}'"
        )
    
    username, password, dsn = match.groups()
    logging.info(f"Parsed SQL*Plus string: User='{username}', DSN='{dsn}'")
    return username, password, dsn

def main():
    """
    Main function to execute the database query tool via sqlplus subprocess.
    Parses arguments, loads config, builds query, executes, and processes results.
    """
    parser = argparse.ArgumentParser(
        description="Connects to an Oracle DB via sqlplus subprocess, builds a SQL query from a JSON config, and fetches data."
    )
    parser.add_argument(
        "json_file",
        type=str,
        help="Path to the JSON file containing table, columns, and filter definitions."
    )
    parser.add_argument(
        "--sqlplus-string",
        type=str,
        help="Optional: A SQL*Plus-style connection string (e.g., 'user/pass@host:port/service_name' or 'user/pass@TNS_ALIAS'). If provided, this overrides environment variables for DB credentials."
    )
    parser.add_argument(
        "--sqlplus-path",
        type=str,
        default="sqlplus",
        help="Optional: The full path to the sqlplus executable (e.g., '/usr/bin/sqlplus'). Defaults to 'sqlplus' (assumes it's in PATH)."
    )

    args = parser.parse_args()
    json_file_path = args.json_file
    sqlplus_conn_string = args.sqlplus_string
    sqlplus_exe_path = args.sqlplus_path

    db_user = DB_USER
    db_pass = DB_PASS
    db_dsn = DB_DSN

    # Determine connection parameters
    if sqlplus_conn_string:
        try:
            db_user, db_pass, db_dsn = parse_sqlplus_connection_string(sqlplus_conn_string)
            logging.info("Using connection details from --sqlplus-string argument.")
        except ValueError as e:
            logging.error(f"Failed to parse --sqlplus-string: {e}. Falling back to environment variables/config.py.")
            # Keep original DB_USER, DB_PASS, DB_DSN from config.py
    else:
        logging.info("Using connection details from environment variables/config.py.")

    db_connection = None # Initialize to None for finally block

    try:
        # 1. Load Query Configuration
        query_config = load_query_config(json_file_path)

        # 2. Initialize Database Connection (via sqlplus subprocess)
        logging.info(f"Connecting via sqlplus for DSN: {db_dsn}")
        db_connection = DatabaseConnection(db_user, db_pass, db_dsn, sqlplus_path=sqlplus_exe_path)
        db_connection.connect() # This now checks sqlplus availability

        # 3. Build SQL Query (for direct embedding into sqlplus input)
        query_builder = QueryBuilder(query_config)
        sql_query_string, _ = query_builder.build_select_query() # Bind params are not used for sqlplus direct input

        # 4. Execute Query via sqlplus subprocess
        raw_csv_output = db_connection.execute_query(sql_query_string)

        if raw_csv_output:
            # 5. Process Results (parse CSV output)
            results_as_dict = DataProcessor.process_csv_output_to_dict(raw_csv_output)
            
            logging.info(f"Query returned {len(results_as_dict)} rows.")
            
            # Print results (you might want to save to a file or process further)
            print("\n--- Query Results ---")
            if results_as_dict:
                for row in results_as_dict:
                    print(row)
            else:
                print("No data found matching the criteria.")
            
        else:
            logging.warning("No raw CSV output received from sqlplus.")

    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        logging.error(f"Execution failed: {e}")
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON file: {e}")
        logging.error(f"Execution failed: {e}")
    except ValueError as e:
        print(f"ERROR: Configuration or parsing issue: {e}")
        logging.error(f"Execution failed: {e}")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: SQL*Plus subprocess failed. See logs for details.")
        logging.error(f"Execution failed due to SQL*Plus error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        logging.exception("An unexpected error occurred during execution:") # Logs traceback
    finally:
        # Ensure the database connection cleanup (interface consistency)
        if db_connection:
            db_connection.close()

if __name__ == "__main__":
    main()

