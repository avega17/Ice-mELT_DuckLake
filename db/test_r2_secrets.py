#!/usr/bin/env python3
"""
R2 Secret Management and Testing Script
Usage: python db/test_r2_secrets.py [command]

Commands:
  create-local    Create local (temporary) R2 secret
  create-md       Create MotherDuck persistent R2 secret  
  list            List all secrets
  test            Test R2 connection
  cleanup         Remove all R2 secrets
"""

import os
import sys
import duckdb
from pathlib import Path

# Add repo root to path for imports
repo_root = Path(__file__).parent.parent
sys.path.append(str(repo_root))

def load_env():
    """Load environment variables from .env file"""
    env_file = repo_root / '.env'
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value.strip('"\'')

def get_r2_credentials():
    """Get R2 credentials from environment"""
    return {
        'key_id': os.getenv('R2_ACCESS_KEY_ID'),
        'secret_key': os.getenv('R2_SECRET_KEY'),
        'account_id': os.getenv('CLOUDFLARE_ACCOUNT_ID'),
        'bucket_name': os.getenv('DUCKLAKE_NAME', 'eo_pv_lakehouse')
    }

def create_local_secret(conn, creds):
    """Create local (temporary) R2 secret"""
    if not all([creds['key_id'], creds['secret_key'], creds['account_id']]):
        print("❌ Missing R2 credentials in environment")
        return False
    
    try:
        conn.execute(f"""
            CREATE OR REPLACE SECRET r2_local (
                TYPE r2,
                KEY_ID '{creds['key_id']}',
                SECRET '{creds['secret_key']}',
                ACCOUNT_ID '{creds['account_id']}'
            )
        """)
        print("✅ Local R2 secret created successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to create local R2 secret: {e}")
        return False

def create_motherduck_secret(conn, creds):
    """Create MotherDuck persistent R2 secret"""
    if not all([creds['key_id'], creds['secret_key'], creds['account_id']]):
        print("❌ Missing R2 credentials in environment")
        return False
    
    try:
        conn.execute(f"""
            CREATE SECRET r2_motherduck IN MOTHERDUCK (
                TYPE r2,
                KEY_ID '{creds['key_id']}',
                SECRET '{creds['secret_key']}',
                ACCOUNT_ID '{creds['account_id']}'
            )
        """)
        print("✅ MotherDuck R2 secret created successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to create MotherDuck R2 secret: {e}")
        return False

def list_secrets(conn):
    """List all secrets"""
    try:
        print("\n📋 Local Secrets:")
        local_secrets = conn.execute("SELECT name, type, scope FROM duckdb_secrets()").fetchall()
        for name, type_, scope in local_secrets:
            print(f"  - {name} ({type_}) [{scope}]")
        
        print("\n📋 MotherDuck Secrets:")
        try:
            md_secrets = conn.execute("SELECT * FROM list_secrets()").fetchall()
            for secret in md_secrets:
                print(f"  - {secret}")
        except Exception as e:
            print(f"  ⚠️  Could not list MotherDuck secrets: {e}")
            
    except Exception as e:
        print(f"❌ Failed to list secrets: {e}")

def test_r2_connection(conn, creds):
    """Test R2 connection"""
    bucket_name = creds['bucket_name']
    
    try:
        print(f"\n🧪 Testing R2 connection to bucket: {bucket_name}")
        
        # Test 1: List files in bucket
        print("  📁 Listing files in bucket...")
        result = conn.execute(f"FROM glob('r2://{bucket_name}/**/*')").fetchall()
        print(f"  ✅ Found {len(result)} files in bucket")
        
        # Test 2: Try to read a specific file (if it exists)
        test_file = f"r2://{bucket_name}/geoparquet/raw_chn_med_res_pv_2024.parquet"
        print(f"  📄 Testing specific file: {test_file}")
        try:
            count_result = conn.execute(f"SELECT COUNT(*) FROM '{test_file}'").fetchone()
            print(f"  ✅ File readable, contains {count_result[0]} records")
        except Exception as e:
            print(f"  ⚠️  File not found or not readable: {e}")
            
    except Exception as e:
        print(f"❌ R2 connection test failed: {e}")

def cleanup_secrets(conn):
    """Remove all R2 secrets"""
    try:
        # List and drop local R2 secrets
        secrets = conn.execute("SELECT name FROM duckdb_secrets() WHERE type = 'r2'").fetchall()
        for (name,) in secrets:
            conn.execute(f"DROP SECRET IF EXISTS {name}")
            print(f"🗑️  Dropped local secret: {name}")
        
        # Try to drop MotherDuck secrets (may fail if not connected)
        try:
            conn.execute("DROP SECRET r2_motherduck FROM MOTHERDUCK")
            print("🗑️  Dropped MotherDuck secret: r2_motherduck")
        except Exception as e:
            print(f"⚠️  Could not drop MotherDuck secret: {e}")
            
    except Exception as e:
        print(f"❌ Failed to cleanup secrets: {e}")

def main():
    load_env()
    creds = get_r2_credentials()
    
    # Connect to DuckDB (local for testing)
    conn = duckdb.connect(':memory:')
    
    # Install required extensions
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    
    command = sys.argv[1] if len(sys.argv) > 1 else 'help'
    
    if command == 'create-local':
        create_local_secret(conn, creds)
        list_secrets(conn)
    elif command == 'create-md':
        create_motherduck_secret(conn, creds)
        list_secrets(conn)
    elif command == 'list':
        list_secrets(conn)
    elif command == 'test':
        create_local_secret(conn, creds)
        test_r2_connection(conn, creds)
    elif command == 'cleanup':
        cleanup_secrets(conn)
    else:
        print(__doc__)

if __name__ == '__main__':
    main()
