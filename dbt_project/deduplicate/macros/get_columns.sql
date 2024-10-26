{% macro get_columns(relation) %}
    {% set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list %}
    {{ return(columns) }}
{% endmacro %}
