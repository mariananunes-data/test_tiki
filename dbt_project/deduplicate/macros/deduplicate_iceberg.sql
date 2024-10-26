-- This macro receives source_table,order_by, unique_key and returns a de-duplicated table. This table will be created using a model.
--source_table: The table you want to deduplicate.
--order_by: The column used to order the rows within each partition.
--unique_key: The column that uniquely identifies a row.
{% macro deduplicate_iceberg(source_table,order_by, unique_key) %}

--Because of compilation and execution some checks need to be made. The dbt_utils has a function to get columns (get_filtered_columns_in_relation) but this wasn't working well in my code, so I replicated some of the logic of this dbt macro to create a macro called get_columns.
    {% set relation =source_table %}
    {% set source_exists = adapter.get_relation(
        database=relation.database,
        schema=relation.schema,
        identifier=relation.identifier
    ) is not none %}

    {% if source_exists %}
        {% set columns = get_columns(relation) %}
--It checks if the column passed in the parameters exists and if there are any nulls in the table.
        {% set query %}
                SELECT COUNT(*) FROM {{ relation }} WHERE {{unique_key}} IS NULL or {{order_by}} IS NULL
        {% endset %}
        {% set null_count = run_query(query).columns[0][0] %}

        {% if null_count > 0 %}
            {{ exceptions.raise_compiler_error("Erro: A tabela '" ~ relation ~ "' contém " ~ null_count ~ " valor(es) nulo(s) entre os parâmetros passados.") }}
        {% endif %}
        {% else %}
        {{ log("A relação não existe ainda.", info=True) }}
        {% set columns = [] %}
    {% endif %}

    
    {% if columns != [] and order_by not in columns %}
          {{ exceptions.raise_compiler_error("Erro: A coluna que foi fornecida no parâmetro order_by ('" ~ order_by ~ "') não existe na tabela de origem.") }}
    {% endif %}
    {% if columns != [] and unique_key not in columns %}
          {{ exceptions.raise_compiler_error("Erro: A coluna que foi fornecida no parâmetro unique_key ('" ~ unique_key ~ "') não existe na tabela de origem.") }}
    {% endif %}


--If all goes well it returns the table without duplicates
    SELECT {{ columns | join(', ') }}
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY {{ unique_key }} ORDER BY {{ order_by }} DESC) AS row_num
        FROM {{ relation }}
    ) AS select_rn
    WHERE row_num = 1 -- AND version = current snapshot 
{% endmacro %}