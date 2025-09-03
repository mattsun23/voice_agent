import sqlite3
import json
import os
from datetime import datetime
from typing import List, Dict, Any


# STEP 1 

def create_database_connection(db_path: str) -> sqlite3.Connection:
    """Create and return a database connection."""
    try:
        conn = sqlite3.connect(db_path)
        print(f"✓ Successfully connected to database: {db_path}")
        return conn
    #safe handling
    except sqlite3.Error as e:
        print(f"✗ Error connecting to database: {e}")
        raise

# STEP 2 

def create_user_metadata_table(conn: sqlite3.Connection) -> None:
    """Create the cloud_ratings table with appropriate schema."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        date_of_birth DATE NOT NULL,
        state TEXT NOT NULL,
        address TEXT NOT NULL,
        phone TEXT,
        email TEXT,
        gender TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        print("✓ Successfully created or verified 'users' table.")
    except sqlite3.Error as e:
        print(f"✗ Error creating 'users' table: {e}")
        raise

# STEP 3 

def load_json_data(json_file_path: str) -> List[Dict[str, Any]]:
    """Load and return data from JSON file."""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        print(f"✓ Successfully loaded {len(data)} records from {json_file_path}")
        return data
    except FileNotFoundError:
        print(f"✗ Error: JSON file not found: {json_file_path}")
        raise
    except json.JSONDecodeError as e:
        print(f"✗ Error decoding JSON: {e}")
        raise

# STEP 4 

def insert_user_metadata(conn: sqlite3.Connection, data: List[Dict[str, Any]]) -> None:
    """Insert user metadata into the users table."""
    insert_sql = """
    INSERT INTO users (
        first_name, last_name, date_of_birth, state, address, phone, email, gender
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        for record in data:
            cursor.execute(insert_sql, (
                record['first_name'],
                record['last_name'],
                record['date_of_birth'],
                record['state'],
                record['address'],
                record.get('phone'),
                record.get('email'),
                record.get('gender')
            ))
        conn.commit()
        print(f"✓ Successfully inserted {len(data)} records into users table")
    except sqlite3.Error as e:
        print(f"✗ Error inserting data: {e}")
        raise

# STEP 5 

def create_users_full_schema_view(conn: sqlite3.Connection) -> None:
    """Create a view that exposes the full schema of the users table."""
    view_sql = """
    CREATE VIEW IF NOT EXISTS users_full_schema AS
    SELECT 
        id,
        first_name,
        last_name,
        date_of_birth,
        state,
        address,
        phone,
        email,
        gender,
        created_at,
        updated_at
    FROM users;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(view_sql)
        conn.commit()
        print("✓ Successfully created users_full_schema view")
    except sqlite3.Error as e:
        print(f"✗ Error creating users_full_schema view: {e}")
        raise

# Step 6: Test Function

def verify_data(conn: sqlite3.Connection) -> None:
    """Verify the imported user data by running some basic queries."""
    print("\n" + "="*50)
    print("DATABASE VERIFICATION")
    print("="*50)
    
    try:
        cursor = conn.cursor()
        
        # Count total records
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        print(f"Total records in 'users' table: {count}")
        
        # Show top 5 most recently created users
        cursor.execute("""
            SELECT id, first_name, last_name, created_at
            FROM users
            ORDER BY created_at DESC
            LIMIT 5
        """)
        
        print("\nTop 5 Most Recently Created Users:")
        print("-" * 45)
        for row in cursor.fetchall():
            print(f"ID: {row[0]}, Name: {row[1]} {row[2]}, Created At: {row[3]}")
        
        # Show gender distribution
        cursor.execute("""
            SELECT gender, COUNT(*) as count 
            FROM users 
            GROUP BY gender
        """)
        
        print("\nGender Distribution:")
        print("-" * 30)
        for row in cursor.fetchall():
            gender = row[0] if row[0] else "Unspecified"
            print(f"{gender}: {row[1]} records")
            
        # Show state distribution (top 5)
        cursor.execute("""
            SELECT state, COUNT(*) as count
            FROM users
            GROUP BY state
            ORDER BY count DESC
            LIMIT 5
        """)
        print("\nTop 5 States by User Count:")
        print("-" * 35)
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]} users")
            
    except sqlite3.Error as e:
        print(f"✗ Error during verification: {e}")


def main():
    """Main function to orchestrate the user database creation process."""
    print("User Metadata Database Creator")
    print("=" * 50)
    
    # Configuration - support both local dev and container environments
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Look for JSON file in multiple locations
    json_file_candidates = [
        os.path.join(script_dir, 'users.json'),
        './users.json',
        '/app/users.json'
    ]
    
    json_file_path = None
    for candidate in json_file_candidates:
        if os.path.exists(candidate):
            json_file_path = candidate
            break
    
    if not json_file_path:
        json_file_path = json_file_candidates[0]  # Use first as default
    
    # Database path - prefer container data directory
    db_path = os.getenv("DB_PATH", "/app/data/users.db")
    if not os.path.exists(os.path.dirname(db_path)):
        # Fallback to script directory for local development
        db_path = os.path.join(script_dir, 'users.db')
    
    # Check if JSON file exists
    if not os.path.exists(json_file_path):
        print(f"✗ Error: JSON file not found at {json_file_path}")
        return
    
    try:
        # Create database connection
        conn = create_database_connection(db_path)
        
        # Create table schema
        create_user_metadata_table(conn)
        
        # Load JSON data
        data = load_json_data(json_file_path)
        
        # Insert data into database
        insert_user_metadata(conn, data)
        
        # Create full schema view
        create_users_full_schema_view(conn)
        
        # Verify the imported data
        verify_data(conn)
        
        print(f"\n✓ Database successfully created at: {db_path}")
        print("✓ You can now query the database using any SQLite client or Python")
        
        # Example queries
        print("\nExample queries you can run:")
        print("1. SELECT * FROM users ORDER BY created_at DESC;")
        print("2. SELECT * FROM users_full_schema;")
        print("3. SELECT state, COUNT(*) FROM users GROUP BY state ORDER BY COUNT(*) DESC;")
        
    except Exception as e:
        print(f"✗ Fatal error: {e}")
        return
    
    finally:
        if 'conn' in locals():
            conn.close()
            print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()
