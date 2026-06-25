
def validate_filters_for_table(
    table: str,
    filters: Optional[dict]
):
    if not filters:
        return {}

    allowed_columns = set(get_table_column_names(table))

    validated_filters = {}

    for frontend_column, values in filters.items():

        db_column = frontend_column

        if frontend_column in SEMANTIC_FILTERS:
            db_column = SEMANTIC_FILTERS[frontend_column].get(table)

        #
        # ignore unsupported semantic filters
        #
        if not db_column:
            continue

        #
        # ignore unknown columns
        #
        if db_column not in allowed_columns:
            continue

        validated_filters[db_column] = values

    return validated_filters



def build_filter_clause(filters):
    if not filters:
        return "", {}

    clauses = []
    params = {}

    idx = 0

    for column, values in filters.items():

        if not values:
            continue

        value_clauses = []

        for value in values:

            param_name = f"filter_{idx}"

            value_clauses.append(
                f'LOWER(CAST(t."{column}" AS TEXT)) LIKE LOWER(:{param_name})'
            )

            params[param_name] = f"%{value}%"

            idx += 1

        clauses.append(
            "(" + " OR ".join(value_clauses) + ")"
        )

    if not clauses:
        return "", {}

    return " AND " + " AND ".join(clauses), params
