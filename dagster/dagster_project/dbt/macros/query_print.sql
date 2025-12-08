{% macro query_print(sql) %}
    {% set results = run_query(sql) %}

    {% if execute %}
        {{ log("=== RESULTS ===", info=True) }}

        {% if results %}
            {% for row in results %}
                {{ log(row, info=True) }}
            {% endfor %}
        {% else %}
            {{ log("Executed query, but without results.", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}
