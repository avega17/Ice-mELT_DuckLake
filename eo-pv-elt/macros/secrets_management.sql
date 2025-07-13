-- DuckDB SQL Macros for R2 Secret Management
-- Reference: https://duckdb.org/docs/stable/configuration/secrets_manager.html
-- Reference: https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-secret/

-- Macro to create a local (temporary) R2 secret
-- Usage: SELECT create_local_r2_secret('my_key_id', 'my_secret_key', 'my_account_id');
{% macro create_local_r2_secret(key_id, secret_key, account_id, secret_name='r2_secret') %}
    CREATE OR REPLACE SECRET {{ secret_name }} (
        TYPE r2,
        KEY_ID '{{ key_id }}',
        SECRET '{{ secret_key }}',
        ACCOUNT_ID '{{ account_id }}'
    );
{% endmacro %}

-- Macro to create a persistent R2 secret in MotherDuck
-- Usage: SELECT create_motherduck_r2_secret('my_key_id', 'my_secret_key', 'my_account_id');
{% macro create_motherduck_r2_secret(key_id, secret_key, account_id, secret_name='r2_secret') %}
    CREATE SECRET {{ secret_name }} IN MOTHERDUCK (
        TYPE r2,
        KEY_ID '{{ key_id }}',
        SECRET '{{ secret_key }}',
        ACCOUNT_ID '{{ account_id }}'
    );
{% endmacro %}

-- Macro to create R2 secret from environment variables (local)
-- Usage: SELECT create_r2_secret_from_env();
{% macro create_r2_secret_from_env(secret_name='r2_secret') %}
    {% set key_id = env_var('R2_ACCESS_KEY_ID') %}
    {% set secret_key = env_var('R2_SECRET_KEY') %}
    {% set account_id = env_var('CLOUDFLARE_ACCOUNT_ID') %}
    
    {{ create_local_r2_secret(key_id, secret_key, account_id, secret_name) }}
{% endmacro %}

-- Macro to create MotherDuck R2 secret from environment variables
-- Usage: SELECT create_motherduck_r2_secret_from_env();
{% macro create_motherduck_r2_secret_from_env(secret_name='r2_secret') %}
    {% set key_id = env_var('R2_ACCESS_KEY_ID') %}
    {% set secret_key = env_var('R2_SECRET_KEY') %}
    {% set account_id = env_var('CLOUDFLARE_ACCOUNT_ID') %}
    
    {{ create_motherduck_r2_secret(key_id, secret_key, account_id, secret_name) }}
{% endmacro %}

-- Macro to list all secrets
-- Usage: SELECT list_all_secrets();
{% macro list_all_secrets() %}
    FROM duckdb_secrets();
{% endmacro %}

-- Macro to list MotherDuck secrets
-- Usage: SELECT list_motherduck_secrets();
{% macro list_motherduck_secrets() %}
    SELECT * FROM list_secrets();
{% endmacro %}

-- Macro to drop a secret
-- Usage: SELECT drop_secret('secret_name');
{% macro drop_secret(secret_name) %}
    DROP SECRET IF EXISTS {{ secret_name }};
{% endmacro %}

-- Macro to drop a MotherDuck secret
-- Usage: SELECT drop_motherduck_secret('secret_name');
{% macro drop_motherduck_secret(secret_name) %}
    DROP SECRET {{ secret_name }} FROM MOTHERDUCK;
{% endmacro %}

-- Macro to test R2 connection with a secret
-- Usage: SELECT test_r2_connection('bucket_name', 'file_path');
{% macro test_r2_connection(bucket_name, file_path='') %}
    {% if file_path %}
        SELECT COUNT(*) as record_count FROM 'r2://{{ bucket_name }}/{{ file_path }}';
    {% else %}
        FROM glob('r2://{{ bucket_name }}/**/*');
    {% endif %}
{% endmacro %}

-- Macro to setup R2 secret based on target (dev vs prod)
-- Usage: SELECT setup_r2_secret_for_target();
{% macro setup_r2_secret_for_target() %}
    {% if target.name == 'prod' %}
        -- For production, create MotherDuck persistent secret
        {{ create_motherduck_r2_secret_from_env('r2_prod_secret') }}
    {% else %}
        -- For development, create local temporary secret
        {{ create_r2_secret_from_env('r2_dev_secret') }}
    {% endif %}
{% endmacro %}
