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

{% macro month_series(start_expression, end_expression, column_name='month_start_date') -%}
  {% if target.type == 'bigquery' %}
    unnest(generate_date_array(cast({{ start_expression }} as date), cast({{ end_expression }} as date), interval 1 month)) as {{ column_name }}
  {% else %}
    generate_series(cast({{ start_expression }} as date), cast({{ end_expression }} as date), interval 1 month) as series({{ column_name }})
  {% endif %}
{%- endmacro %}

{% macro date_series(start_expression, end_expression, column_name='date_day') -%}
  {% if target.type == 'bigquery' %}
    unnest(generate_date_array(cast({{ start_expression }} as date), cast({{ end_expression }} as date), interval 1 day)) as {{ column_name }}
  {% else %}
    generate_series(cast({{ start_expression }} as date), cast({{ end_expression }} as date), interval 1 day) as series({{ column_name }})
  {% endif %}
{%- endmacro %}

{% macro date_add_months(expression, months) -%}
  {% if target.type == 'bigquery' %}
    date_add(cast({{ expression }} as date), interval {{ months }} month)
  {% else %}
    cast({{ expression }} as date) + interval {{ months }} month
  {% endif %}
{%- endmacro %}

{% macro day_diff(later_expression, earlier_expression) -%}
  {% if target.type == 'bigquery' %}
    date_diff(cast({{ later_expression }} as date), cast({{ earlier_expression }} as date), day)
  {% else %}
    date_diff('day', cast({{ earlier_expression }} as date), cast({{ later_expression }} as date))
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

{% macro clean_label_text(expression) -%}
  {% if target.type == 'bigquery' %}
    nullif(
      trim(
        regexp_replace(
          regexp_replace({{ cast_string(expression) }}, r'[\t\r\n]+', ' '),
          r' +',
          ' '
        )
      ),
      ''
    )
  {% else %}
    nullif(
      trim(
        regexp_replace(
          regexp_replace({{ cast_string(expression) }}, '[\t\r\n]+', ' ', 'g'),
          ' +',
          ' ',
          'g'
        )
      ),
      ''
    )
  {% endif %}
{%- endmacro %}

{% macro canonical_country_iso3(expression) -%}
  {%- set cleaned_code = clean_label_text(expression) -%}
  case
    when {{ cleaned_code }} is null then null
    when upper({{ cleaned_code }}) in ('NULL', 'N/A', 'NA', 'NONE', 'UNKNOWN', 'UNK', 'NAN') then null
    when upper({{ cleaned_code }}) = 'ROM' then 'ROU'
    when upper({{ cleaned_code }}) = 'UK' then 'GBR'
    when upper({{ cleaned_code }}) = 'US' then 'USA'
    when upper({{ cleaned_code }}) = 'SA' then 'ZAF'
    when upper({{ cleaned_code }}) = 'CN' then 'CHN'
    else upper({{ cleaned_code }})
  end
{%- endmacro %}

{% macro canonical_country_name(name_expr, iso3_expr='null') -%}
  {%- set cleaned_name = clean_label_text(name_expr) -%}
  {%- set canonical_iso3 = canonical_country_iso3(iso3_expr) -%}
  case
    when {{ canonical_iso3 }} = 'BEL' then 'Belgium'
    when {{ canonical_iso3 }} = 'FRA' then 'France'
    when {{ canonical_iso3 }} = 'ZAF' then 'South Africa'
    when {{ canonical_iso3 }} = 'CHN' then 'China'
    when {{ canonical_iso3 }} = 'IND' then 'India'
    when {{ canonical_iso3 }} = 'PAN' then 'Panama'
    when {{ canonical_iso3 }} = 'USA' then 'United States'
    when {{ canonical_iso3 }} = 'RUS' then 'Russia'
    when {{ canonical_iso3 }} = 'TUR' then 'Turkey'
    when {{ canonical_iso3 }} = 'ROU' then 'Romania'
    when {{ canonical_iso3 }} = 'NLD' then 'Netherlands'
    when {{ canonical_iso3 }} = 'EGY' then 'Egypt'
    when {{ cleaned_name }} is null then null
    when upper({{ cleaned_name }}) in ('NULL', 'N/A', 'NA', 'NONE', 'UNKNOWN', 'UNK', 'NAN') then null
    when lower({{ cleaned_name }}) in ('turkiye', 'türkiye', 'turkie', 'turkish republic') then 'Turkey'
    when lower({{ cleaned_name }}) = 'russian federation' then 'Russia'
    when lower({{ cleaned_name }}) in ('u.s.a.', 'usa', 'united states of america') then 'United States'
    when lower({{ cleaned_name }}) = 'holland' then 'Netherlands'
    when lower({{ cleaned_name }}) = 'republic of south africa' then 'South Africa'
    when lower({{ cleaned_name }}) = 'metropolitan france' then 'France'
    else {{ cleaned_name }}
  end
{%- endmacro %}

{% macro looker_country_name(name_expr, iso3_expr='null') -%}
  {%- set cleaned_name = clean_label_text(name_expr) -%}
  {%- set canonical_iso3 = canonical_country_iso3(iso3_expr) -%}
  case
    when {{ canonical_iso3 }} = 'IND' then 'India'
    when {{ canonical_iso3 }} = 'PAN' then 'Panama'
    when {{ canonical_iso3 }} = 'USA' then 'United States'
    when {{ canonical_iso3 }} = 'RUS' then 'Russia'
    when {{ canonical_iso3 }} = 'TUR' then 'Turkey'
    when {{ canonical_iso3 }} = 'VNM' then 'Vietnam'
    when {{ canonical_iso3 }} = 'KOR' then 'South Korea'
    when {{ canonical_iso3 }} = 'COD' then 'Democratic Republic of the Congo'
    when {{ canonical_iso3 }} = 'HKG' then 'Hong Kong'
    when {{ canonical_iso3 }} = 'MAC' then 'Macao'
    when {{ cleaned_name }} is null then null
    when lower({{ cleaned_name }}) in ('turkiye', 'türkiye') then 'Turkey'
    when lower({{ cleaned_name }}) = 'russian federation' then 'Russia'
    when lower({{ cleaned_name }}) in ('usa', 'u.s.a.', 'united states of america') then 'United States'
    when lower({{ cleaned_name }}) = 'viet nam' then 'Vietnam'
    when lower({{ cleaned_name }}) = 'rep. of korea' then 'South Korea'
    when lower({{ cleaned_name }}) = 'dem. rep. of the congo' then 'Democratic Republic of the Congo'
    when lower({{ cleaned_name }}) = 'china, hong kong sar' then 'Hong Kong'
    when lower({{ cleaned_name }}) = 'china, macao sar' then 'Macao'
    else {{ canonical_country_name(name_expr, iso3_expr) }}
  end
{%- endmacro %}

{% macro canonical_chokepoint_key(name_expr) -%}
  {%- set cleaned_name = clean_label_text(name_expr) -%}
  case
    when {{ cleaned_name }} is null then null
    when lower({{ cleaned_name }}) in ('hormuz', 'hormuz strait', 'strait of hormuz') then 'Strait of Hormuz'
    when lower({{ cleaned_name }}) in (
      'bab el-mandeb',
      'bab el-mandeb strait',
      'bab el mandeb',
      'bab el mandeb strait',
      'bab-el-mandeb',
      'bab-el-mandeb strait'
    ) then 'Bab el-Mandeb'
    when lower({{ cleaned_name }}) in ('panama', 'panama canal') then 'Panama Canal'
    when lower({{ cleaned_name }}) in ('malacca', 'malacca strait', 'strait of malacca') then 'Malacca Strait'
    when lower({{ cleaned_name }}) in ('gibraltar', 'gibraltar strait', 'strait of gibraltar') then 'Gibraltar Strait'
    when lower({{ cleaned_name }}) in ('suez', 'suez canal') then 'Suez Canal'
    when lower({{ cleaned_name }}) = 'cape of good hope' then 'Cape of Good Hope'
    when lower({{ cleaned_name }}) = 'turkish straits' then 'Turkish Straits'
    when lower({{ cleaned_name }}) = 'open sea' then 'Open Sea'
    else {{ cleaned_name }}
  end
{%- endmacro %}

{% macro canonicalize_chokepoint_name(name_expr) -%}
  case
    when {{ canonical_chokepoint_key(name_expr) }} is null then null
    when {{ canonical_chokepoint_key(name_expr) }} = 'Bab el-Mandeb' then 'Bab el-Mandeb Strait'
    when {{ canonical_chokepoint_key(name_expr) }} = 'Malacca Strait' then 'Strait of Malacca'
    when {{ canonical_chokepoint_key(name_expr) }} = 'Gibraltar Strait' then 'Strait of Gibraltar'
    else {{ canonical_chokepoint_key(name_expr) }}
  end
{%- endmacro %}

{% macro canonical_chokepoint_id(name_expr) -%}
  {{ hash_text("lower(" ~ canonical_chokepoint_key(name_expr) ~ ")") }}
{%- endmacro %}

{% macro geography_from_wkb(expression) -%}
  {% if target.type == 'bigquery' %}
    ST_GEOGFROMWKB({{ expression }})
  {% else %}
    null
  {% endif %}
{%- endmacro %}

{% macro geography_point(longitude_expr, latitude_expr) -%}
  {% if target.type == 'bigquery' %}
    ST_GEOGPOINT({{ longitude_expr }}, {{ latitude_expr }})
  {% else %}
    null
  {% endif %}
{%- endmacro %}
