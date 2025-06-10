# data_processor.py

import logging
import csv
import io
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level='INFO', format='%(asctime)s - %(levelname)s - %(message)s')

class DataProcessor:
    """
    A class to process raw CSV string output from sqlplus into a list of dictionaries.
    Each dictionary represents a row, with column names as keys.
    """

    @staticmethod
    def process_csv_output_to_dict(csv_data_string: str) -> List[Dict[str, Any]]:
        """
        Parses a raw CSV string (from sqlplus SET MARKUP CSV ON) and converts
        it into a list of dictionaries.

        Args:
            csv_data_string (str): The raw string output from sqlplus, expected to be in CSV format.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                                  represents a row from the query results.
                                  Returns an empty list if the input string is empty or invalid.
        """
        if not csv_data_string:
            logging.warning("No CSV data string provided for processing. Returning empty list.")
            return []

        results = []
        try:
            # Use io.StringIO to treat the string as a file for csv.reader/DictReader
            csvfile = io.StringIO(csv_data_string)
            
            # csv.DictReader automatically uses the first row as headers
            reader = csv.DictReader(csvfile)
            
            # Process each row
            for row in reader:
                # DictReader's keys are strings. Convert values if necessary (e.g., numbers)
                # For simplicity, we keep all values as strings as they come from CSV.
                # Further type conversion (e.g., "123" to 123) can be added here if needed.
                results.append(dict(row)) # Convert OrderedDict to regular dict

            logging.info(f"Processed {len(results)} rows from CSV data.")
            return results

        except csv.Error as e:
            logging.error(f"Error parsing CSV data: {e}")
            logging.debug(f"Problematic CSV data (first 200 chars):\n{csv_data_string[:200]}...")
            return []
        except Exception as e:
            logging.error(f"An unexpected error occurred during CSV processing: {e}")
            return []

# Example usage (for testing purposes, not part of main execution flow)
if __name__ == '__main__':
    # Mock CSV data similar to what sqlplus SET MARKUP CSV ON would produce
    mock_csv_data = """
"EMPLOYEE_ID","FIRST_NAME","LAST_NAME","EMAIL","SALARY"
"100","Steven","King","SKING","24000"
"101","Neena","Kochhar","NKOCHHAR","17000"
"102","Lex","De Haan","LDEHAAN","17000"
"103","Alexander","Hunold","AHUNOLD","9000"
""".strip() # .strip() to remove leading/trailing newlines if any

    print("\n--- Processing Mock CSV Data ---")
    processed_data = DataProcessor.process_csv_output_to_dict(mock_csv_data)
    for row_dict in processed_data:
        print(row_dict)

    # Test with empty data
    print("\n--- Processing Empty CSV Data ---")
    empty_data = DataProcessor.process_csv_output_to_dict("")
    print(empty_data)

    # Test with malformed CSV (e.g., missing quotes if column contains commas)
    malformed_csv_data = """
HEADER1,HEADER2
value1,"value2, with comma"
value3,value4 without quotes,extra
"""
    print("\n--- Processing Malformed CSV Data ---")
    malformed_data = DataProcessor.process_csv_output_to_dict(malformed_csv_data)
    print(malformed_data)

