{% macro cast_string(expression) -%}
  cast({{ expression }} as {{ dbt.type_string() }})
{%- endmacro %}

{% macro cast_int(expression) -%}
  cast({{ expression }} as {{ dbt.type_int() }})
{%- endmacro %}

{% macro cast_float(expression) -%}
  cast({{ expression }} as {{ dbt.type_float() }})
{%- endmacro %}

{% macro safe_cast(expression, data_type) -%}
  {% if target.type == 'bigquery' %}
    safe_cast({{ expression }} as {{ data_type }})
  {% else %}
    try_cast({{ expression }} as {{ data_type }})
  {% endif %}
{%- endmacro %}

{% macro regex_full_match(expression, pattern) -%}
  {% if target.type == 'bigquery' %}
    regexp_contains({{ cast_string(expression) }}, r'{{ pattern }}')
  {% else %}
    regexp_full_match({{ cast_string(expression) }}, '{{ pattern }}')
  {% endif %}
{%- endmacro %}

{% macro month_start_from_year_month(expression) -%}
  {% if target.type == 'bigquery' %}
    safe_cast(concat({{ cast_string(expression) }}, '-01') as date)
  {% else %}
    cast(strptime({{ cast_string(expression) }} || '-01', '%Y-%m-%d') as date)
  {% endif %}
{%- endmacro %}

{% macro year_start_date(expression) -%}
  {% if target.type == 'bigquery' %}
    safe_cast(concat({{ cast_string(expression) }}, '-01-01') as date)
  {% else %}
    cast(strptime({{ cast_string(expression) }} || '-01-01', '%Y-%m-%d') as date)
  {% endif %}
{%- endmacro %}

{% macro year_month_from_date(expression) -%}
  {% if target.type == 'bigquery' %}
    format_date('%Y-%m', cast({{ expression }} as date))
  {% else %}
    strftime({{ expression }}, '%Y-%m')
  {% endif %}
{%- endmacro %}

{% macro year_int_from_date(expression) -%}
  cast(extract(year from cast({{ expression }} as date)) as {{ dbt.type_int() }})
{%- endmacro %}

{% macro month_int_from_date(expression) -%}
  cast(extract(month from cast({{ expression }} as date)) as {{ dbt.type_int() }})
{%- endmacro %}

{% macro quarter_int_from_date(expression) -%}
  cast(extract(quarter from cast({{ expression }} as date)) as {{ dbt.type_int() }})
{%- endmacro %}

{% macro period_int_from_date(expression) -%}
  {% if target.type == 'bigquery' %}
    cast(format_date('%Y%m', cast({{ expression }} as date)) as {{ dbt.type_int() }})
  {% else %}
    cast(strftime({{ expression }}, '%Y%m') as {{ dbt.type_int() }})
  {% endif %}
{%- endmacro %}

{% macro truncate_to_month(expression) -%}
  {% if target.type == 'bigquery' %}
    cast(date_trunc(cast({{ expression }} as date), month) as date)
  {% else %}
    cast(date_trunc('month', cast({{ expression }} as date)) as date)
  {% endif %}
{%- endmacro %}

{% macro date_add_months(expression, month_count) -%}
  {% if month_count >= 0 %}
    date_add(cast({{ expression }} as date), interval {{ month_count }} month)
  {% else %}
    date_sub(cast({{ expression }} as date), interval {{ -month_count }} month)
  {% endif %}
{%- endmacro %}

{% macro month_series(start_expression, end_expression, column_name='month_start_date') -%}
  {% if target.type == 'bigquery' %}
    unnest(generate_date_array(cast({{ start_expression }} as date), cast({{ end_expression }} as date), interval 1 month)) as {{ column_name }}
  {% else %}
    generate_series(cast({{ start_expression }} as date), cast({{ end_expression }} as date), interval 1 month) as series({{ column_name }})
  {% endif %}
{%- endmacro %}

{% macro bool_or(expression) -%}
  {% if target.type == 'bigquery' %}
    logical_or({{ expression }})
  {% else %}
    bool_or({{ expression }})
  {% endif %}
{%- endmacro %}

{% macro array_agg_distinct(expression) -%}
  {% if target.type == 'bigquery' %}
    array_agg(distinct {{ expression }} ignore nulls)
  {% else %}
    list(distinct {{ expression }})
  {% endif %}
{%- endmacro %}

{% macro hash_text(expression) -%}
  {% if target.type == 'bigquery' %}
    to_hex(md5(cast({{ expression }} as string)))
  {% else %}
    md5(cast({{ expression }} as varchar))
  {% endif %}
{%- endmacro %}
