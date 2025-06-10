# db_connector.py

import logging
import subprocess
import os

# Configure logging
from config import LOG_LEVEL
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseConnection:
    """
    A class to manage Oracle database connections by invoking sqlplus as a subprocess.
    This bypasses direct Python driver connections, leveraging a pre-configured sqlplus environment.
    """

    def __init__(self, user: str, password: str, dsn: str, sqlplus_path: str = 'sqlplus'):
        """
        Initializes the DatabaseConnection with connection parameters for sqlplus.

        Args:
            user (str): Database username.
            password (str): Database password.
            dsn (str): Data Source Name (e.g., 'host:port/service_name' or 'TNS_ALIAS').
            sqlplus_path (str): The path to the sqlplus executable. Defaults to 'sqlplus'
                                as it might be in the system's PATH.
        """
        self.user = user
        self.password = password
        self.dsn = dsn
        self.sqlplus_path = sqlplus_path
        self.connection_string = f"{self.user}/{self.password}@{self.dsn}"
        logging.info(f"DatabaseConnection initialized for SQL*Plus path: {sqlplus_path}, DSN: {dsn}")

    def connect(self):
        """
        In this subprocess-based approach, 'connect' primarily checks if sqlplus is available.
        A real connection is established with each query execution.
        """
        try:
            # Check if sqlplus executable is available
            subprocess.run([self.sqlplus_path, '-V'], check=True, capture_output=True, text=True)
            logging.info(f"sqlplus executable found at '{self.sqlplus_path}'.")
            return True
        except FileNotFoundError:
            logging.error(f"sqlplus executable not found at '{self.sqlplus_path}'. "
                          "Please ensure it's in your system's PATH or provide the full path.")
            raise
        except subprocess.CalledProcessError as e:
            logging.error(f"Error checking sqlplus version: {e.stderr.strip()}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred while checking sqlplus: {e}")
            raise

    def close(self):
        """
        No explicit connection to close for sqlplus subprocess calls,
        as each call is independent. This method is here for interface consistency.
        """
        logging.info("No explicit sqlplus connection to close (subprocess mode).")

    def execute_query(self, query: str) -> str:
        """
        Executes a SQL query against the database using sqlplus subprocess.

        Args:
            query (str): The SQL query string to execute.
                         Note: Bind parameters are handled by sqlplus internally,
                         but the query itself must be self-contained for sqlplus.

        Returns:
            str: The raw CSV output from sqlplus, including column headers.
                 Returns an empty string if the query fails or returns no data.

        Raises:
            subprocess.CalledProcessError: If the sqlplus command returns a non-zero exit code.
            Exception: For other unexpected errors during subprocess execution.
        """
        # SQL*Plus commands to format output as CSV with headers
        # SET MARKUP CSV ON is for Oracle 12.2+
        # SET PAGESIZE 0: No pagination
        # SET FEEDBACK OFF: No "X rows selected" message
        # SET HEADING ON: Keep column headers (needed for CSV parsing)
        # SET TERMOUT ON, SET TRIMSPOOL ON: Standard for clean output
        # LINESIZE 32767: Max line size to prevent wrapping
        sqlplus_commands = [
            "SET PAGESIZE 0",
            "SET FEEDBACK OFF",
            "SET HEADING ON",
            "SET TERMOUT ON",
            "SET TRIMSPOOL ON",
            "SET LINESIZE 32767",
            "SET MARKUP CSV ON", # Crucial for CSV output
            query,
            "EXIT;" # Ensure sqlplus exits after the query
        ]
        
        # Combine commands into a single string to be piped to sqlplus
        sqlplus_input = "\n".join(sqlplus_commands)

        command = [self.sqlplus_path, '-S', self.connection_string] # -S for silent mode

        logging.debug(f"Running sqlplus command: {' '.join(command)}")
        logging.debug(f"Piping SQL: {sqlplus_input}")

        try:
            process = subprocess.run(
                command,
                input=sqlplus_input,
                capture_output=True,
                text=True, # Decode stdout/stderr as text
                check=True # Raise CalledProcessError if return code is non-zero
            )
            logging.info("sqlplus query executed successfully.")
            
            # The output will contain the CSV data.
            # We filter out any leading non-CSV lines (e.g., connection banners if -S isn't fully silent or errors)
            # This is a basic filter; more robust parsing might be needed for complex banners.
            # For SET MARKUP CSV ON, the first line is usually the headers.
            output_lines = process.stdout.strip().splitlines()
            
            # Find the first line that looks like a CSV header (usually all uppercase for column names)
            # Or assume the first non-empty line after sqlplus setup is the header if SET HEADING ON
            
            # A more robust check might be needed for actual CSV content start.
            # For simplicity, we'll return the whole stdout and let DataProcessor handle it.
            return process.stdout.strip()
            
        except subprocess.CalledProcessError as e:
            logging.error(f"SQL*Plus command failed with error code {e.returncode}.")
            logging.error(f"STDOUT: {e.stdout.strip()}")
            logging.error(f"STDERR: {e.stderr.strip()}")
            raise ValueError(f"SQL*Plus execution error: {e.stderr.strip() or e.stdout.strip()}") from e
        except FileNotFoundError:
            logging.error(f"sqlplus executable not found at '{self.sqlplus_path}'. Ensure it's in PATH.")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during sqlplus execution: {e}")
            raise

# Example usage (for testing purposes, not part of main execution flow)
if __name__ == '__main__':
    # WARNING: This test block directly calls sqlplus.
    # Ensure sqlplus is in your PATH or update the sqlplus_path argument.
    # Set environment variables DB_USER, DB_PASS, DB_DSN before running.
    
    # For a real test, replace with your actual DB details
    test_user = os.getenv("DB_USER", "system")
    test_pass = os.getenv("DB_PASS", "oracle")
    test_dsn = os.getenv("DB_DSN", "localhost:1521/XEPDB1")

    db_conn = DatabaseConnection(test_user, test_pass, test_dsn)
    try:
        db_conn.connect() # Check sqlplus availability

        print("\n--- Testing simple SELECT from DUAL ---")
        csv_output = db_conn.execute_query("SELECT 'Hello from SQL*Plus!' AS MESSAGE FROM DUAL;")
        print("Raw CSV Output:\n", csv_output)

        print("\n--- Testing invalid query ---")
        try:
            db_conn.execute_query("SELECT NON_EXISTENT_COLUMN FROM DUAL;")
        except ValueError as ve:
            print(f"Caught expected error: {ve}")

    except Exception as e:
        print(f"Failed to run db_connector test due to: {e}")
    finally:
        db_conn.close()

