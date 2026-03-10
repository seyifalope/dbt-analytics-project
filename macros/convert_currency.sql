{% macro convert_currency(column_name, exchange_rate = 0.79) %}
    ({{ column_name}} * {{ exchange_rate}})
 {% endmacro %}   