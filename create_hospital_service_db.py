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

# In theory, I should be creating a relational database with hospitals, doctors, and deparments. 

def create_hospital_service_schema(conn: sqlite3.Connection) -> None:
    """Create hospitals, doctors, and departments tables with relationships."""
    try:
        cursor = conn.cursor()

        # Hospitals table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS hospitals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            phone TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Departments table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            description TEXT,
            hospital_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (hospital_id) REFERENCES hospitals(id)
        );
        """)

        # Doctors table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS doctors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            phone TEXT,
            speciality TEXT,
            hospital_id INTEGER NOT NULL,
            department_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (hospital_id) REFERENCES hospitals(id),
            FOREIGN KEY (department_id) REFERENCES departments(id)
        );
        """)

        conn.commit()
        print("✓ Successfully created or verified 'hospitals', 'departments', and 'doctors' tables.")
    except sqlite3.Error as e:
        print(f"✗ Error creating hospital service schema: {e}")
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
def insert_hospitals(conn: sqlite3.Connection, hospitals_data: List[Dict[str, Any]]) -> None:
    """
    Insert hospital records into the hospitals table.

    Each hospital record must include a 'phone' field, as it is required by the schema.
    """
    insert_sql = """
    INSERT INTO hospitals (name, city, state, phone)
    VALUES (?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        for record in hospitals_data:
            cursor.execute(insert_sql, (
                record['name'],
                record['city'],
                record['state'],
                record.get('phone')
            ))
        conn.commit()
        print(f"✓ Successfully inserted {len(hospitals_data)} records into hospitals table")
    except sqlite3.Error as e:
        print(f"✗ Error inserting hospitals: {e}")
        raise

def insert_departments(conn: sqlite3.Connection, departments_data: List[Dict[str, Any]]) -> None:
    """Insert department records into the departments table."""
    insert_sql = """
    INSERT INTO departments (name, phone, description, hospital_id)
    VALUES (?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        for record in departments_data:
            cursor.execute(insert_sql, (
                record['name'],
                record.get('phone'),
                record.get('description'),
                record['hospital_id']
            ))
        conn.commit()
        print(f"✓ Successfully inserted {len(departments_data)} records into departments table")
    except sqlite3.Error as e:
        print(f"✗ Error inserting departments: {e}")
        raise

def insert_doctors(conn: sqlite3.Connection, doctors_data: List[Dict[str, Any]]) -> None:
    """Insert doctor records into the doctors table."""
    insert_sql = """
    INSERT INTO doctors (first_name, last_name, phone, speciality, hospital_id, department_id)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        for record in doctors_data:
            cursor.execute(insert_sql, (
                record['first_name'],
                record['last_name'],
                record.get('phone'),
                record.get('speciality'),
                record['hospital_id'],
                record['department_id']
            ))
        conn.commit()
        print(f"✓ Successfully inserted {len(doctors_data)} records into doctors table")
    except sqlite3.Error as e:
        print(f"✗ Error inserting doctors: {e}")
        raise

# STEP 5 

def create_doctors_full_schema_view(conn: sqlite3.Connection) -> None:
    """Create a view that exposes doctors with their department and hospital info."""
    view_sql = """
    CREATE VIEW IF NOT EXISTS doctors_full_schema AS
    SELECT 
        d.id AS doctor_id,
        d.first_name,
        d.last_name,
        d.phone AS doctor_phone,
        d.speciality,
        d.created_at AS doctor_created_at,
        d.updated_at AS doctor_updated_at,
        dep.id AS department_id,
        dep.name AS department_name,
        dep.phone AS department_phone,
        dep.description AS department_description,
        h.id AS hospital_id,
        h.name AS hospital_name,
        h.city AS hospital_city,
        h.state AS hospital_state,
        h.phone AS hospital_phone
    FROM doctors d
    JOIN departments dep ON d.department_id = dep.id
    JOIN hospitals h ON d.hospital_id = h.id;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(view_sql)
        conn.commit()
        print("✓ Successfully created doctors_full_schema view")
    except sqlite3.Error as e:
        print(f"✗ Error creating doctors_full_schema view: {e}")
        raise

# Step 6: Test Function

def verify_data(conn: sqlite3.Connection) -> None:
    """Verify the imported hospital, department, and doctor data by running some basic queries."""
    print("\n" + "="*50)
    print("DATABASE VERIFICATION")
    print("="*50)
    
    try:
        cursor = conn.cursor()
        
        # Count total hospitals
        cursor.execute("SELECT COUNT(*) FROM hospitals")
        hospital_count = cursor.fetchone()[0]
        print(f"Total records in 'hospitals' table: {hospital_count}")
        
        # Count total departments
        cursor.execute("SELECT COUNT(*) FROM departments")
        department_count = cursor.fetchone()[0]
        print(f"Total records in 'departments' table: {department_count}")
        
        # Count total doctors
        cursor.execute("SELECT COUNT(*) FROM doctors")
        doctor_count = cursor.fetchone()[0]
        print(f"Total records in 'doctors' table: {doctor_count}")
        
        # Show top 5 most recently added doctors
        cursor.execute("""
            SELECT d.id, d.first_name, d.last_name, d.speciality, d.created_at, h.name AS hospital_name, dep.name AS department_name
            FROM doctors d
            JOIN hospitals h ON d.hospital_id = h.id
            JOIN departments dep ON d.department_id = dep.id
            ORDER BY d.created_at DESC
            LIMIT 5
        """)
        print("\nTop 5 Most Recently Added Doctors:")
        print("-" * 60)
        for row in cursor.fetchall():
            print(f"ID: {row[0]}, Name: {row[1]} {row[2]}, Speciality: {row[3]}, Hospital: {row[5]}, Department: {row[6]}, Created At: {row[4]}")
        
        # Show number of departments per hospital (top 5)
        cursor.execute("""
            SELECT h.name, COUNT(dep.id) as dept_count
            FROM hospitals h
            LEFT JOIN departments dep ON dep.hospital_id = h.id
            GROUP BY h.id
            ORDER BY dept_count DESC
            LIMIT 5
        """)
        print("\nTop 5 Hospitals by Department Count:")
        print("-" * 45)
        for row in cursor.fetchall():
            print(f"Hospital: {row[0]}, Departments: {row[1]}")
        
        # Show number of doctors per department (top 5)
        cursor.execute("""
            SELECT dep.name, h.name, COUNT(d.id) as doctor_count
            FROM departments dep
            JOIN hospitals h ON dep.hospital_id = h.id
            LEFT JOIN doctors d ON d.department_id = dep.id
            GROUP BY dep.id
            ORDER BY doctor_count DESC
            LIMIT 5
        """)
        print("\nTop 5 Departments by Doctor Count:")
        print("-" * 50)
        for row in cursor.fetchall():
            print(f"Department: {row[0]} (Hospital: {row[1]}), Doctors: {row[2]}")
        
    except sqlite3.Error as e:
        print(f"✗ Error during verification: {e}")


def main():
    """Main function to orchestrate the hospital service database creation process."""
    print("Hospital Service Database Creator")
    print("=" * 50)

    # Configuration - support both local dev and container environments
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Look for JSON files in multiple locations
    hospitals_json_candidates = [
        os.path.join(script_dir, 'hospitals.json'),
        './hospitals.json',
        '/app/hospitals.json'
    ]
    departments_json_candidates = [
        os.path.join(script_dir, 'departments.json'),
        './departments.json',
        '/app/departments.json'
    ]
    doctors_json_candidates = [
        os.path.join(script_dir, 'doctors.json'),
        './doctors.json',
        '/app/doctors.json'
    ]

    def find_json_file(candidates):
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
        return candidates[0]  # Default to first if not found

    hospitals_json_path = find_json_file(hospitals_json_candidates)
    departments_json_path = find_json_file(departments_json_candidates)
    doctors_json_path = find_json_file(doctors_json_candidates)

    # Database path - prefer container data directory
    db_path = os.getenv("DB_PATH", "/app/data/hospital_service.db")
    if not os.path.exists(os.path.dirname(db_path)):
        # Fallback to script directory for local development
        db_path = os.path.join(script_dir, 'hospital_service.db')

    # Check if JSON files exist
    missing_files = []
    for path, label in [
        (hospitals_json_path, "hospitals"),
        (departments_json_path, "departments"),
        (doctors_json_path, "doctors"),
    ]:
        if not os.path.exists(path):
            missing_files.append(f"{label}.json (expected at {path})")
    if missing_files:
        print("✗ Error: The following JSON files are missing:")
        for f in missing_files:
            print(f"  - {f}")
        return

    try:
        # Create database connection
        conn = create_database_connection(db_path)

        # Create table schema
        create_hospital_service_schema(conn)

        # Load JSON data
        hospitals_data = load_json_data(hospitals_json_path)
        departments_data = load_json_data(departments_json_path)
        doctors_data = load_json_data(doctors_json_path)

        # Insert data into database
        insert_hospitals(conn, hospitals_data)
        insert_departments(conn, departments_data)
        insert_doctors(conn, doctors_data)

        # Create full schema view
        create_doctors_full_schema_view(conn)

        # Verify the imported data
        verify_data(conn)

        print(f"\n✓ Database successfully created at: {db_path}")
        print("✓ You can now query the database using any SQLite client or Python")

        # Example queries
        print("\nExample queries you can run:")
        print("1. SELECT * FROM hospitals ORDER BY created_at DESC;")
        print("2. SELECT * FROM doctors_full_schema;")
        print("3. SELECT city, COUNT(*) FROM hospitals GROUP BY city ORDER BY COUNT(*) DESC;")

    except Exception as e:
        print(f"✗ Fatal error: {e}")
        return

    finally:
        if 'conn' in locals():
            conn.close()
            print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()
