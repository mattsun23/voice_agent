# Copilot Instructions for Parlance Hospital Service Codebase

## Overview
This project manages a hospital service database and provides a FastAPI interface for agentic decision support. The core logic is in Python scripts for database creation, population, and verification, with a FastAPI app for future API endpoints.

## Architecture
- **Database Layer:**
  - `create_hospital_service_db.py` orchestrates SQLite DB creation, schema setup, data import (from JSON), and verification.
  - Data model: `hospitals`, `departments`, `doctors` tables with foreign key relationships. Data is loaded from `hospitals.json`, `departments.json`, `doctors.json`.
  - A SQL view (`doctors_full_schema`) joins doctors, departments, and hospitals for reporting.
- **API Layer:**
  - `fastapi.py` initializes a FastAPI app. (No endpoints yet, but app is ready for extension.)

## Developer Workflows
- **Database Build/Reset:**
  - Run `python create_hospital_service_db.py` to (re)create the SQLite DB, load data, and verify integrity.
  - Data files must be present: `hospitals.json`, `departments.json`, `doctors.json` (see script for search paths).
- **Testing:**
  - Use `test_queries.py` (if present) or run ad-hoc queries against the generated `hospital_service.db`.
- **API Development:**
  - Extend `fastapi.py` with new endpoints. Use the initialized `app` object.

## Project Conventions
- **Data Loading:** Always insert hospitals first, then departments, then doctors (to satisfy foreign key constraints).
- **Schema Changes:** Update both the table creation and the corresponding insert functions.
- **Error Handling:** Print errors to console and re-raise for critical failures.
- **File Locations:**
  - Database and JSON files are expected in the project root or `/app/` (for container use).

## Integration Points
- **External:**
  - SQLite (no external DB server required)
  - FastAPI (for future API endpoints)
- **Internal:**
  - Data flows: JSON → Python dicts → SQLite tables

## Examples
- To verify DB: `python create_hospital_service_db.py` (prints summary and sample queries)
- To extend API: add routes to `fastapi.py` using the `app` object

## Key Files
- `create_hospital_service_db.py`: DB schema, data import, verification
- `fastapi.py`: FastAPI app definition
- `hospitals.json`, `departments.json`, `doctors.json`: Data sources

---
For questions about data flow, schema, or extending the API, see the comments in `create_hospital_service_db.py` and `fastapi.py`.
