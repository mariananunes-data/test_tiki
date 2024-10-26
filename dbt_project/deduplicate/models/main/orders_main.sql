{{ config(materialized='table')}}

{% set source_table = ref('orders') %}
{% set unique_key = 'ORDER_ID' %}
{% set order_by = 'UPDATED_AT' %}

{{ deduplicate_iceberg(source_table, order_by, unique_key) }}