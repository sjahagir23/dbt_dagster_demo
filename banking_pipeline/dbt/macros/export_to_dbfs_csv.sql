{% macro export_to_dbfs_csv(model_name) %}

{% set relation = ref(model_name) %}

{% set query %}
COPY INTO '/dbfs/tmp/banking_pipeline/account_summary'
FROM (
    SELECT * FROM {{ relation }}
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true')
OVERWRITE = TRUE
{% endset %}

{{ log("Exporting " ~ model_name ~ " to DBFS", info=True) }}

{{ run_query(query) }}

{% endmacro %}
