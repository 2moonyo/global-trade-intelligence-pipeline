{% macro safe_divide(numerator, denominator) -%}
  case
    when {{ denominator }} is null or {{ denominator }} = 0 then null
    else {{ numerator }} / {{ denominator }}
  end
{%- endmacro %}

{% macro fx_inverse_rate(expression) -%}
  {{ safe_divide('1.0', expression) }}
{%- endmacro %}

{% macro fx_cross_rate_via_anchor(base_to_anchor_expression, anchor_to_quote_expression) -%}
  case
    when {{ base_to_anchor_expression }} is null or {{ anchor_to_quote_expression }} is null then null
    else {{ base_to_anchor_expression }} * {{ anchor_to_quote_expression }}
  end
{%- endmacro %}
