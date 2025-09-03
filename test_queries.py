import sqlite3

def run_sample_queries(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\n--- All Hospitals ---")
    for row in cursor.execute("SELECT id, name, city, state FROM hospitals ORDER BY name;"):
        print(row)

    print("\n--- All Departments with Hospital Name ---")
    for row in cursor.execute("""
        SELECT dep.id, dep.name, h.name AS hospital_name
        FROM departments dep
        JOIN hospitals h ON dep.hospital_id = h.id
        ORDER BY h.name, dep.name;
    """):
        print(row)

    print("\n--- All Doctors with Department and Hospital ---")
    for row in cursor.execute("""
        SELECT d.first_name, d.last_name, d.speciality, dep.name AS department, h.name AS hospital
        FROM doctors d
        JOIN departments dep ON d.department_id = dep.id
        JOIN hospitals h ON d.hospital_id = h.id
        ORDER BY h.name, dep.name, d.last_name;
    """):
        print(row)

    print("\n--- Doctors Full Schema View ---")
    for row in cursor.execute("SELECT * FROM doctors_full_schema LIMIT 5;"):
        print(row)

    print("\n--- Number of Doctors per Hospital ---")
    for row in cursor.execute("""
        SELECT h.name, COUNT(d.id) as doctor_count
        FROM hospitals h
        LEFT JOIN doctors d ON d.hospital_id = h.id
        GROUP BY h.id
        ORDER BY doctor_count DESC;
    """):
        print(row)

    conn.close()

if __name__ == "__main__":
    # Update the path if needed
    db_path = "hospital_service.db"
    run_sample_queries(db_path)