{% macro get_parquet_path(source_name, table_name) %}
    {%- set source_table_map = {
        'bronze': {
            'dim_items': 'C:/Users/David/my_dbt_project/ga4_data/dim/dim_items/*.parquet',
            'dim_users': 'C:/Users/David/my_dbt_project/ga4_data/dim/dim_users/*.parquet',
            'fact_event_items': 'C:/Users/David/my_dbt_project/ga4_data/fct/fact_event_items/*.parquet',
            'fact_events': 'C:/Users/David/my_dbt_project/ga4_data/fct/fact_events/*.parquet'
        }
    } -%}

    {%- if source_name in source_table_map and table_name in source_table_map[source_name] -%}
        '{{ source_table_map[source_name][table_name] }}'
    {%- else -%}
        {{ exceptions.raise_compiler_error("Path for source '" ~ source_name ~ "', table '" ~ table_name ~ "' not found in get_parquet_path macro.") }}
    {%- endif -%}
{% endmacro %}