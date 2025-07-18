# R2 Secret Management for DuckDB and MotherDuck

This guide covers how to manage Cloudflare R2 secrets for accessing cloud storage from DuckDB and MotherDuck.

## Overview

We use DuckDB's native R2 secret support to authenticate with Cloudflare R2 storage. This allows us to use the `r2://` protocol for reading and writing data.

## Prerequisites

Ensure you have the following environment variables set:
- `R2_ACCESS_KEY_ID`: Your Cloudflare R2 access key
- `R2_SECRET_KEY`: Your Cloudflare R2 secret key  
- `CLOUDFLARE_ACCOUNT_ID`: Your Cloudflare account ID
- `DUCKLAKE_NAME`: Your R2 bucket name (default: `eo_pv_lakehouse`)

## Secret Types

### 1. Local (Temporary) Secrets
- **Scope**: Session-based, not persisted
- **Use case**: Development, testing, dbt Python models
- **Lifetime**: Exists only for the current DuckDB session

### 2. MotherDuck Persistent Secrets
- **Scope**: Stored in MotherDuck, persist across sessions
- **Use case**: Production workloads, shared team access
- **Lifetime**: Persists until explicitly deleted


### In dbt Python Models

The raw models automatically create R2 secrets based on the target:

```python
# For development (local secret)
if target_name == 'dev':
    secret_name = "r2_dev_secret"

# For production (could use MotherDuck persistent secret)
if target_name == 'prod':
    secret_name = "r2_prod_secret"
```

### Manual SQL Commands

```sql
-- Create local R2 secret
CREATE OR REPLACE SECRET r2_local (
    TYPE r2,
    KEY_ID 'your_access_key_id',
    SECRET 'your_secret_key',
    ACCOUNT_ID 'your_account_id'
);

-- Create MotherDuck persistent secret
CREATE SECRET r2_motherduck IN MOTHERDUCK (
    TYPE r2,
    KEY_ID 'your_access_key_id',
    SECRET 'your_secret_key',
    ACCOUNT_ID 'your_account_id'
);

-- List secrets
SELECT * FROM duckdb_secrets();

-- Test R2 connection
FROM glob('r2://eo_pv_lakehouse/**/*');
```

## File Structure

```
eo-pv-elt/
├── macros/
│   └── secrets_management.sql    # dbt macros for secret management
├── sql/
│   └── r2_secrets.sql           # Raw SQL commands and examples
└── models/raw/
    └── *.py                     # Python models with automatic secret creation

db/
└── test_r2_secrets.py          # Testing and management script
```

## Troubleshooting

### Common Issues

1. **Missing credentials**: Ensure all environment variables are set
2. **Account ID format**: Use the full account ID from Cloudflare dashboard
3. **Bucket access**: Verify R2 API token has read/write permissions for the bucket

### Debug Commands

```bash
# Check environment variables
echo $R2_ACCESS_KEY_ID
echo $CLOUDFLARE_ACCOUNT_ID

# Test secret creation
python db/test_r2_secrets.py create-local

# List active secrets
python db/test_r2_secrets.py list
```

### Error Messages

- `Missing R2 credentials`: Check environment variables
- `Failed to create R2 SECRET`: Verify account ID and credentials
- `HTTP 403 Forbidden`: Check R2 API token permissions
- `Bucket not found`: Verify bucket name and access

## Best Practices

1. **Development**: Use local secrets for faster iteration
2. **Production**: Use MotherDuck persistent secrets for consistency
3. **Security**: Never hardcode credentials in code
4. **Testing**: Use the test script to verify connections before deployment
5. **Cleanup**: Remove unused secrets to avoid confusion

## References

- [DuckDB Secrets Manager](https://duckdb.org/docs/stable/configuration/secrets_manager.html)
- [DuckDB Cloudflare R2 Import](https://duckdb.org/docs/stable/guides/network_cloud_storage/cloudflare_r2_import.html)
- [MotherDuck CREATE SECRET](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-secret/)
- [MotherDuck LIST SECRETS](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/list-secrets/)
