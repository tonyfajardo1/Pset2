{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Macro personalizado para generar nombres de schema.
        Si el modelo tiene un schema custom (silver, gold), usa ese directamente.
        Si no tiene, usa el schema por defecto del profile.
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
