-- R2 Secret Management SQL Commands
-- Reference: https://duckdb.org/docs/stable/configuration/secrets_manager.html
-- Reference: https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-secret/

-- =============================================================================
-- LOCAL (TEMPORARY) SECRETS - Session-based, not persisted
-- =============================================================================

-- Create local R2 secret from environment variables
CREATE OR REPLACE SECRET r2_local (
    TYPE r2,
    KEY_ID '{{ env_var("R2_ACCESS_KEY_ID") }}',
    SECRET '{{ env_var("R2_SECRET_KEY") }}',
    ACCOUNT_ID '{{ env_var("CLOUDFLARE_ACCOUNT_ID") }}'
);

-- Create local R2 secret with explicit values (for testing)
-- CREATE OR REPLACE SECRET r2_test (
--     TYPE r2,
--     KEY_ID 'your_access_key_id',
--     SECRET 'your_secret_access_key',
--     ACCOUNT_ID 'your_account_id'
-- );

-- =============================================================================
-- MOTHERDUCK PERSISTENT SECRETS - Stored in MotherDuck, persist across sessions
-- =============================================================================

-- Create MotherDuck R2 secret from environment variables
CREATE SECRET r2_motherduck IN MOTHERDUCK (
    TYPE r2,
    KEY_ID '{{ env_var("R2_ACCESS_KEY_ID") }}',
    SECRET '{{ env_var("R2_SECRET_KEY") }}',
    ACCOUNT_ID '{{ env_var("CLOUDFLARE_ACCOUNT_ID") }}'
);

-- =============================================================================
-- SECRET MANAGEMENT COMMANDS
-- =============================================================================

-- List all local secrets
SELECT * FROM duckdb_secrets();

-- List MotherDuck secrets (only works when connected to MotherDuck)
-- SELECT * FROM list_secrets();

-- Drop local secret
-- DROP SECRET IF EXISTS r2_local;

-- Drop MotherDuck secret
-- DROP SECRET r2_motherduck FROM MOTHERDUCK;

-- =============================================================================
-- TESTING R2 CONNECTION
-- =============================================================================

-- Test R2 connection by listing files in bucket
-- FROM glob('r2://eo_pv_lakehouse/**/*');

-- Test R2 connection by reading a specific file
-- SELECT COUNT(*) FROM 'r2://eo_pv_lakehouse/geoparquet/raw_chn_med_res_pv_2024.parquet';

-- Test R2 connection with different bucket
-- FROM glob('r2://{{ env_var("DUCKLAKE_NAME") }}/**/*');

-- =============================================================================
-- EXAMPLE USAGE IN PYTHON MODELS
-- =============================================================================

/*
In your Python dbt models, you can use these patterns:

# For local development (temporary secret)
session.execute('''
    CREATE OR REPLACE SECRET r2_local (
        TYPE r2,
        KEY_ID '{}',
        SECRET '{}',
        ACCOUNT_ID '{}'
    );
'''.format(
    os.getenv('R2_ACCESS_KEY_ID'),
    os.getenv('R2_SECRET_KEY'),
    os.getenv('CLOUDFLARE_ACCOUNT_ID')
))

# For production (MotherDuck persistent secret)
session.execute('''
    CREATE SECRET r2_prod IN MOTHERDUCK (
        TYPE r2,
        KEY_ID '{}',
        SECRET '{}',
        ACCOUNT_ID '{}'
    );
'''.format(
    os.getenv('R2_ACCESS_KEY_ID'),
    os.getenv('R2_SECRET_KEY'),
    os.getenv('CLOUDFLARE_ACCOUNT_ID')
))

# Test the connection
session.execute("FROM glob('r2://eo_pv_lakehouse/**/*');")
*/
