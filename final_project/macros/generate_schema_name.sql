{% macro generate_schema_name(custom_schema_name, node) %}
    {% set default_schema = target.schema %}
    {% set target_name = target.name %}
    {% set resource_type = node.resource_type %}
    
    {# Jika resource adalah seed, selalu pakai custom_schema jika ada, kalau tidak pakai default_schema #}
    {% if resource_type == 'seed' %}
        {{ (custom_schema_name or default_schema) | trim }}
    
    {# Jika custom_schema_name tidak di-set, pakai default schema #}
    {% elif custom_schema_name is none %}
        {{ default_schema }}
    
    {# Jika di environment production, tambahkan prefix default_schema_ sebelum custom schema #}
    {% elif target_name == 'prod' %}
        {{ default_schema ~ '_' ~ (custom_schema_name | trim) }}
    
    {# Di environment selain prod, pakai custom_schema langsung #}
    {% else %}
        {{ custom_schema_name | trim }}
    {% endif %}
{% endmacro %}