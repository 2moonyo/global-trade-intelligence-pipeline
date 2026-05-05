{% macro format_compact_number(expression) -%}
  case
    when {{ expression }} is null then null
    when abs({{ expression }}) >= 1000000000000 then concat(format('%.2f', {{ expression }} / 1000000000000.0), 'T')
    when abs({{ expression }}) >= 1000000000 then concat(format('%.2f', {{ expression }} / 1000000000.0), 'B')
    when abs({{ expression }}) >= 1000000 then concat(format('%.2f', {{ expression }} / 1000000.0), 'M')
    when abs({{ expression }}) >= 1000 then concat(format('%.2f', {{ expression }} / 1000.0), 'K')
    else format('%.0f', {{ expression }})
  end
{%- endmacro %}
