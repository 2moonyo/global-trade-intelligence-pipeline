{% macro rolling_corr(x_expression, y_expression, partition_by, order_by, window_size) -%}
  {% if target.type in ['bigquery', 'duckdb'] %}
    corr({{ x_expression }}, {{ y_expression }}) over (
      partition by {{ partition_by }}
      order by {{ order_by }}
      rows between {{ window_size - 1 }} preceding and current row
    )
  {% else %}
    null
  {% endif %}
{%- endmacro %}
