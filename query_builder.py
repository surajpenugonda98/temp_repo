import logging
from typing import Dict, List, Any, Tuple

logging.basicConfig(level='INFO', format='%(asctime)s - %(levelname)s - %(message)s')

class QueryBuilder:
    def __init__(self, query_config: Dict[str, Any]):
        self.config = query_config
        self._validate_config()
        logging.info("QueryBuilder initialized with configuration.")

    def _validate_config(self):
        if "table" not in self.config or not self.config["table"]:
            raise ValueError("Query configuration must specify a 'table'.")
        if "columns" not in self.config or not isinstance(self.config["columns"], list):
            raise ValueError("Query configuration must specify 'columns' as a list.")
        if not self.config["columns"]:
             logging.warning("No columns specified. Query will select all columns (*).")

    def _build_select_clause(self) -> str:
        columns = self.config.get("columns")
        if not columns:
            return "*"
        return ", ".join([self._sanitize_identifier(col) for col in columns])

    def _build_from_clause(self) -> str:
        table = self._sanitize_identifier(self.config["table"])
        return f"FROM {table}"

    def _sanitize_identifier(self, identifier: str) -> str:
        if not all(c.isalnum() or c == '_' for c in identifier):
            logging.warning(f"Identifier '{identifier}' contains non-alphanumeric characters or underscores. "
                            f"Consider if it needs double quotes in Oracle, e.g., \"{identifier}\". "
                            "Basic sanitization applied.")
        return identifier
    
    def _format_value_for_sql(self, value: Any) -> str:
        if isinstance(value, str):
            return f"'{value.replace(\"'\", \"''\")}'"
        elif value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "1" if value else "0"
        else:
            return str(value)

    def _build_single_condition(self, condition: Dict[str, Any]) -> str:
        column = self._sanitize_identifier(condition["column"])
        operator = condition["operator"].upper()
        value = condition.get("value")

        if operator == "IN":
            if not isinstance(value, list) or not value:
                raise ValueError(f"Filter for column '{column}' with 'IN' operator requires a non-empty list value.")
            formatted_values = [self._format_value_for_sql(item) for item in value]
            return f"{column} IN ({', '.join(formatted_values)})"
        elif operator == "BETWEEN":
            if not isinstance(value, list) or len(value) != 2:
                raise ValueError(f"Filter for column '{column}' with 'BETWEEN' operator requires a list of two values.")
            val1 = self._format_value_for_sql(value[0])
            val2 = self._format_value_for_sql(value[1])
            return f"{column} BETWEEN {val1} AND {val2}"
        elif operator in ["IS NULL", "IS NOT NULL"]:
            return f"{column} {operator}"
        else:
            formatted_value = self._format_value_for_sql(value)
            return f"{column} {operator} {formatted_value}"

    def _build_condition_group(self, conditions_list: List[Dict[str, Any]], default_operator: str = "AND") -> str:
        group_conditions = []
        operator_to_use = default_operator

        for item in conditions_list:
            if "logical_operator" in item and "conditions" in item:
                nested_operator = item["logical_operator"].upper()
                nested_conditions = item["conditions"]
                if not nested_conditions:
                    logging.warning(f"Empty condition list for logical group with operator '{nested_operator}'. Skipping.")
                    continue
                
                nested_group_sql = self._build_condition_group(nested_conditions, nested_operator)
                if nested_group_sql:
                    group_conditions.append(f"({nested_group_sql})")
            elif "column" in item and "operator" in item:
                group_conditions.append(self._build_single_condition(item))
            else:
                logging.warning(f"Unrecognized filter structure: {item}. Skipping.")
        
        if not group_conditions:
            return ""

        return f" {operator_to_use} ".join(group_conditions)


    def _build_where_clause(self) -> str:
        filters = self.config.get("filters", [])
        if not filters:
            return ""

        where_sql = self._build_condition_group(filters, "AND")
        return f"WHERE {where_sql}" if where_sql else ""


    def _build_order_by_clause(self) -> str:
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
        limit = self.config.get("limit")
        if limit is None or not isinstance(limit, int) or limit <= 0:
            return ""
        return f"FETCH NEXT {limit} ROWS ONLY"

    def build_select_query(self) -> Tuple[str, Dict[str, Any]]:
        select_clause = self._build_select_clause()
        from_clause = self._build_from_clause()
        where_clause = self._build_where_clause()
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
        return final_query, {}

if __name__ == '__main__':
    image_like_config = {
        "table": "netcool.reporter_status_1",
        "columns": [
            "ELEMENT", "ALERTKEY", "ALERTNAME", "SEVERITYDESC", "SUMMARY", "TALLY",
            "TECHNOLOGY", "MANAGERCLASS", "AMOCLASS", "CLEAREDBY", "CLEARINGREASON",
            "FAULTOWNER", "FIRSTOCCURRENCE", "LASTOCCURRENCE", "IDENTIFIER", "SERVERSERIAL"
        ],
        "filters": [
            {"column": "FAULTOWNER", "operator": "=", "value": " 18 "},
            {"column": "MANAGERCLASS", "operator": "=", "value": " Mavenir_MWP_IMS "},
            {
                "logical_operator": "OR",
                "conditions": [
                    {"column": "CLEAREDTIME", "operator": "IS NULL"},
                    {"column": "CLEAREDTIME", "operator": "=", "value": "01-JAN-1970 "}
                ]
            }
        ],
        "order_by": [],
        "limit": None
    }

    builder_image = QueryBuilder(image_like_config)
    sql_query_image, params_image = builder_image.build_select_query()
    print("\n--- Generated Query matching image's logic ---")
    print("SQL Query:\n", sql_query_image)
    print("Bind Parameters (empty for sqlplus subprocess):", params_image)

    complex_config = {
        "table": "MY_APP_LOGS",
        "columns": ["LOG_ID", "TIMESTAMP", "MESSAGE", "SEVERITY", "COMPONENT"],
        "filters": [
            {"column": "SEVERITY", "operator": "IN", "value": ["ERROR", "CRITICAL"]},
            {
                "logical_operator": "AND",
                "conditions": [
                    {"column": "TIMESTAMP", "operator": ">=", "value": "2023-01-01"},
                    {"column": "TIMESTAMP", "operator": "<=", "value": "2022-12-31"},
                    {
                        "logical_operator": "OR",
                        "conditions": [
                            {"column": "MESSAGE", "operator": "LIKE", "value": "%failed%"},
                            {"column": "COMPONENT", "operator": "=", "value": "AuthService"}
                        ]
                    }
                ]
            }
        ],
        "order_by": [{"column": "TIMESTAMP", "direction": "DESC"}],
        "limit": 100
    }
    builder_complex = QueryBuilder(complex_config)
    sql_query_complex, params_complex = builder_complex.build_select_query()
    print("\n--- Generated Query with complex nesting ---")
    print("SQL Query:\n", sql_query_complex)
    print("Bind Parameters (empty for sqlplus subprocess):", params_complex)

