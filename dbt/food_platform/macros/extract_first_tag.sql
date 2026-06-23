{% macro extract_first_tag(column_name) %}
    TRIM(
            REPLACE(
                REPLACE(
                    SPLIT_PART(
                        REPLACE(REPLACE(brands_tags, '[', ''), ']', ''),
                        ',', 1
                    ),
                '"',''),
            '''', '')
        )
{% endmacro %}