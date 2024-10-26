{{ config(materialized='table')}}

{% set source_table = ref('customers') %}
{% set unique_key = 'ID' %} --new
{% set order_by = 'UPDATED_AT' %}

{{ deduplicate_iceberg(source_table, order_by, unique_key) }}