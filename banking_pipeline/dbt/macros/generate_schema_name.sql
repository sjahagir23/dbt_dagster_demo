{% macro generate_schema_name(custom_schema_name, node) %}
    {% do log("USING CUSTOM SCHEMA MACRO", info=True) %}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{% endmacro %}
