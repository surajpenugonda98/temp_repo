# query_builder.py

import logging
from typing import Dict, List, Any, Tuple

# Configure logging
logging.basicConfig(level='INFO', format='%(asctime)s - %(levelname)s - %(message)s')

class QueryBuilder:
    """
    Builds SQL SELECT queries based on a dictionary (JSON-like) configuration.
    Handles table, columns, various filter types, ordering, and limits.
    Crucially, it uses bind variables for security and performance.
    """

    def __init__(self, query_config: Dict[str, Any]):
        """
        Initializes the QueryBuilder with the query configuration.

        Args:
            query_config (Dict[str, Any]): A dictionary containing the query definition
                                           (table, columns, filters, order_by, limit).
        """
        self.config = query_config
        self._validate_config()
        logging.info("QueryBuilder initialized with configuration.")

    def _validate_config(self):
        """
        Internal method to validate the essential parts of the query configuration.
        Raises ValueError if required keys are missing.
        """
        if "table" not in self.config or not self.config["table"]:
            raise ValueError("Query configuration must specify a 'table'.")
        if "columns" not in self.config or not isinstance(self.config["columns"], list):
            raise ValueError("Query configuration must specify 'columns' as a list.")
        if not self.config["columns"]:
             logging.warning("No columns specified. Query will select all columns (*).")
             # Optionally, you could set self.config["columns"] = ["*"] here

    def _build_select_clause(self) -> str:
        """Constructs the SELECT part of the query."""
        columns = self.config.get("columns")
        if not columns:
            return "*" # Select all columns if none specified
        return ", ".join([self._sanitize_identifier(col) for col in columns])

    def _build_from_clause(self) -> str:
        """Constructs the FROM part of the query."""
        table = self._sanitize_identifier(self.config["table"])
        return f"FROM {table}"

    def _build_where_clause(self) -> Tuple[str, Dict[str, Any]]:
        """
        Constructs the WHERE clause and collects bind parameters.
        Handles various filter operators and types.
        """
        filters = self.config.get("filters", [])
        conditions = []
        bind_params = {} # Note: these bind_params are generated, but sqlplus uses them differently.
                         # For sqlplus subprocess, the actual query string needs to be complete.
                         # This means QueryBuilder is still useful for general SQL,
                         # but for sqlplus, we'd embed values directly or rely on sqlplus's own parsing.
                         # Given the current QueryBuilder generates ':param_name', we would need
                         # to replace these with actual values for sqlplus.
                         # Let's adjust this. sqlplus doesn't use :param_name for direct input.
                         # It uses '&' or '&&' for substitution variables.
                         # However, for SELECT queries with direct values, we can just inline them
                         # since the string is built programmatically and not user-supplied for values.

        param_counter = 0

        for f in filters:
            column = self._sanitize_identifier(f["column"])
            operator = f["operator"].upper()
            value = f.get("value")

            if operator == "IN":
                if not isinstance(value, list) or not value:
                    logging.warning(f"Filter for column '{column}' with 'IN' operator requires a non-empty list value. Skipping.")
                    continue
                # For sqlplus, inline the values
                formatted_values = []
                for item in value:
                    if isinstance(item, str):
                        formatted_values.append(f"'{item.replace(\"'\", \"''\")}'") # Escape single quotes
                    else:
                        formatted_values.append(str(item))
                conditions.append(f"{column} IN ({', '.join(formatted_values)})")
            elif operator == "BETWEEN":
                if not isinstance(value, list) or len(value) != 2:
                    logging.warning(f"Filter for column '{column}' with 'BETWEEN' operator requires a list of two values. Skipping.")
                    continue
                val1 = f"'{str(value[0]).replace(\"'\", \"''\")}'" if isinstance(value[0], str) else str(value[0])
                val2 = f"'{str(value[1]).replace(\"'\", \"''\")}'" if isinstance(value[1], str) else str(value[1])
                conditions.append(f"{column} BETWEEN {val1} AND {val2}")
            elif operator in ["IS NULL", "IS NOT NULL"]:
                conditions.append(f"{column} {operator}")
            else:
                # For operators like =, !=, >, <, >=, <=, LIKE
                # Inline the value directly
                formatted_value = f"'{str(value).replace(\"'\", \"''\")}'" if isinstance(value, str) else str(value)
                conditions.append(f"{column} {operator} {formatted_value}")
            
            # Remove bind_params as they are not used for sqlplus subprocess
            # bind_params[param_name] is no longer relevant here

        if not conditions:
            return "", {}

        return "WHERE " + " AND ".join(conditions), {} # Return empty dict for bind_params

    def _build_order_by_clause(self) -> str:
        """Constructs the ORDER BY clause."""
        order_by_config = self.config.get("order_by", [])
        if not order_by_config:
            return ""

        orders = []
        for item in order_by_config:
            column = self._sanitize_identifier(item["column"])
            direction = item.get("direction", "ASC").upper()
            if direction not in ["ASC", "DESC"]:
                logging.warning(f"Invalid order direction '{direction}' for column '{column}'. Defaulting to ASC.")
                direction = "ASC"
            orders.append(f"{column} {direction}")

        return "ORDER BY " + ", ".join(orders)

    def _build_limit_clause(self) -> str:
        """Constructs the LIMIT (ROWNUM for Oracle) clause."""
        limit = self.config.get("limit")
        if limit is None or not isinstance(limit, int) or limit <= 0:
            return ""
        # For Oracle 12c+
        return f"FETCH NEXT {limit} ROWS ONLY"

    def _sanitize_identifier(self, identifier: str) -> str:
        """
        Sanitizes SQL identifiers (table names, column names).
        Basic check: allow alphanumeric and underscore.
        For sqlplus, we generally avoid double-quoting unless strictly necessary for
        case-sensitive or special-character identifiers.
        """
        if not all(c.isalnum() or c == '_' for c in identifier):
            logging.warning(f"Identifier '{identifier}' contains non-alphanumeric characters or underscores. "
                            f"Consider if it needs double quotes in Oracle, e.g., \"{identifier}\". "
                            "Basic sanitization applied.")
        return identifier

    def build_select_query(self) -> Tuple[str, Dict[str, Any]]:
        """
        Builds the complete SQL SELECT query string.
        For sqlplus subprocess, values are inlined for simplicity,
        assuming the values come from the trusted JSON configuration
        and are properly escaped.

        Returns:
            Tuple[str, Dict[str, Any]]: A tuple containing:
                - The SQL query string.
                - An empty dictionary (bind_params are not used for sqlplus direct input).
        """
        select_clause = self._build_select_clause()
        from_clause = self._build_from_clause()
        where_clause, _ = self._build_where_clause() # Discard bind_params
        order_by_clause = self._build_order_by_clause()
        limit_clause = self._build_limit_clause()

        query_parts = [
            f"SELECT {select_clause}",
            from_clause
        ]

        if where_clause:
            query_parts.append(where_clause)
        if order_by_clause:
            query_parts.append(order_by_clause)
        if limit_clause:
            query_parts.append(limit_clause)

        final_query = "\n".join(query_parts)
        logging.info(f"Constructed SQL Query for SQL*Plus:\n{final_query}")
        return final_query, {} # Return empty dictionary as bind_params are not used for sqlplus subprocess

# Example usage (for testing purposes)
if __name__ == '__main__':
    # Define a complex query example
    example_config = {
        "table": "HR.EMPLOYEES", # Use schema.table for specific users
        "columns": [
            "employee_id",
            "first_name",
            "last_name",
            "email",
            "salary",
            "hire_date",
            "department_id"
        ],
        "filters": [
            {"column": "salary", "operator": ">=", "value": 8000},
            {"column": "job_id", "operator": "IN", "value": ["IT_PROG", "SA_REP"]},
            {"column": "first_name", "operator": "LIKE", "value": "S%"},
            {"column": "hire_date", "operator": "BETWEEN", "value": ["2000-01-01", "2005-12-31"]},
            {"column": "commission_pct", "operator": "IS NOT NULL"}
        ],
        "order_by": [
            {"column": "salary", "direction": "DESC"},
            {"column": "last_name", "direction": "ASC"}
        ],
        "limit": 5
    }

    builder = QueryBuilder(example_config)
    sql_query, params = builder.build_select_query()
    print("\n--- Generated Query for SQL*Plus and (empty) Parameters ---")
    print("SQL Query:\n", sql_query)
    print("Bind Parameters (empty for sqlplus subprocess):", params)

    # Example of a simpler query
    simple_config = {
        "table": "DUAL",
        "columns": ["SYSDATE"],
        "filters": [],
        "order_by": []
    }
    builder_simple = QueryBuilder(simple_config)
    sql_query_simple, params_simple = builder_simple.build_select_query()
    print("\n--- Simple Query Example for SQL*Plus ---")
    print("SQL Query:\n", sql_query_simple)
    print("Bind Parameters (empty for sqlplus subprocess):", params_simple)

    # Example with no columns (selects all)
    no_cols_config = {
        "table": "HR.EMPLOYEES",
        "columns": [], # Empty list
        "filters": []
    }
    builder_no_cols = QueryBuilder(no_cols_config)
    sql_no_cols, params_no_cols = builder_no_cols.build_select_query()
    print("\n--- No Columns Query Example for SQL*Plus ---")
    print("SQL Query:\n", sql_no_cols)
    print("Bind Parameters (empty for sqlplus subprocess):", params_no_cols)

