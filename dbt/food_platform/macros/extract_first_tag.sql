{% macro extract_first_tag(column_name) %}
    TRIM(
            REPLACE(
                REPLACE(
                    SPLIT_PART(
                        REPLACE(REPLACE({{ column_name }}, '[', ''), ']', ''),
                        ',', 1
                    ),
                '"',''),
            '''', '')
        )
{% endmacro %}