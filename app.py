from fastapi import FastAPI, HTTPException
import sqlite3
from typing import Any, Dict, List
import os

from dotenv import load_dotenv
load_dotenv()


# Initialize FastAPI app

app = FastAPI(
    title="Parlance AI-Infused Pipeline",
    description="An AI-powered healthcare assistant that helps find doctor contact information using natural language queries. Compatible with Watson Orchestrate for enterprise workflow automation.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    contact={
        "name": "Parlance AI Team",
        "email": "support@parlance.ai",
    },
    license_info={
        "name": "MIT",
    },
)


@app.get("/hospital/{hospital_id}", 
         summary="Get Hospital Details",
         description="Retrieve comprehensive hospital information including departments and doctors by hospital ID",
         response_description="Hospital details with associated departments and doctors",
         tags=["Hospital Information"])
def get_hospital_details(hospital_id: int) -> Dict[str, Any]:
    """
    Get detailed information about a hospital including all departments and doctors.
    
    - **hospital_id**: The unique identifier for the hospital
    
    Returns hospital information with:
    - Basic hospital details (name, location, phone)
    - All departments in the hospital
    - All doctors working at the hospital
    """
    db_path = os.getenv("DB_PATH", "hospital_service.db")
    if not os.path.exists(db_path):
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hospital_service.db")
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # Get hospital
        cursor.execute("SELECT * FROM hospitals WHERE id = ?", (hospital_id,))
        hospital = cursor.fetchone()
        if not hospital:
            raise HTTPException(status_code=404, detail="Hospital not found")
        hospital_dict = dict(hospital)
        # Get departments
        cursor.execute("SELECT * FROM departments WHERE hospital_id = ?", (hospital_id,))
        departments = [dict(row) for row in cursor.fetchall()]
        # Get doctors
        cursor.execute("SELECT * FROM doctors WHERE hospital_id = ?", (hospital_id,))
        doctors = [dict(row) for row in cursor.fetchall()]
        hospital_dict["departments"] = departments
        hospital_dict["doctors"] = doctors
        return hospital_dict
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

import os 

@app.get("/user/{user_id}",
         summary="Get User Details", 
         description="Retrieve user information by user ID",
         response_description="Complete user profile information",
         tags=["User Information"])
def get_user_details(user_id: int) -> Dict[str, Any]:
    """
    Get detailed user information by user ID.
    
    - **user_id**: The unique identifier for the user
    
    Returns user profile including:
    - Personal information (name, date of birth)
    - Contact details (phone, email, address)
    - Demographics (state, gender)
    """
    db_path = os.getenv("DB_PATH", "users.db")
    if not os.path.exists(db_path):
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # Get user (all fields as in create_user_db.py)
        cursor.execute("SELECT id, first_name, last_name, date_of_birth, state, address, phone, email, gender, created_at, updated_at FROM users WHERE id = ?", (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return dict(user)
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

from fastapi import Body

from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ibm_watsonx_ai import APIClient
from ibm_watsonx_ai import Credentials
from ibm_watsonx_ai.foundation_models import ModelInference

import os 


class WatsonxRequest(BaseModel):
    string_input: str = "I would like to get in touch with Dr. Smith from Emergency"
    hospital_id: int
    
    class Config:
        schema_extra = {
            "example": {
                "string_input": "I need to contact Dr. Smith from the Emergency department",
                "hospital_id": 1
            }
        }

class DoctorPhoneResponse(BaseModel):
    response: str
    
    class Config:
        schema_extra = {
            "example": {
                "response": "+1-888-555-2000"
            }
        }

@app.post("/call_watsonx",
          summary="Find Doctor Phone Number",
          description="Use AI to extract doctor phone numbers from natural language queries",
          response_description="Doctor's phone number extracted by AI",
          response_model=DoctorPhoneResponse,
          tags=["AI Assistant"])
def call_watsonx(request: WatsonxRequest) -> DoctorPhoneResponse:
    """
    Process natural language request to find doctor contact information.
    
    - **string_input**: Natural language query about finding a doctor
    - **hospital_id**: ID of the hospital to search in
    
    Uses IBM Watson AI to intelligently extract the relevant doctor's phone number
    from the hospital's database based on the user's natural language request.
    """
    watsonx_ai_url = os.getenv("WATSONX_URL")
    apikey = os.getenv("WATSONX_APIKEY")

    print("WATSONX_URL:", watsonx_ai_url)
    print("WATSONX_APIKEY:", apikey)

    if not watsonx_ai_url or not apikey:
        raise HTTPException(status_code=500, detail="WATSONX_URL and/or WATSONX_APIKEY environment variables are not set.")

    credentials = Credentials(url=watsonx_ai_url, api_key=apikey)
    client = APIClient(credentials)

    model = ModelInference(
        model_id="ibm/granite-3-3-8b-instruct",
        api_client=client,
        params={"decoding_method": "greedy", "max_new_tokens": 100},
        project_id="18c28599-3d2e-430d-974a-9f4b30bb623e",
        verify=False
    )

    try:
        input_of_hospitals = get_hospital_details(request.hospital_id)
    except HTTPException as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve hospital data: {e.detail}")

    prompt = (
        f"Get me the phone number of the doctor that the user is requesting. Here is the user's request: {request.string_input}. "
        f"""Return ONLY the phone number. NO EXPLANATION. 

        Here is an example of a correct response: 123-456-7890.
        
        Here is the data: {input_of_hospitals}."""
    )
    
    response = model.generate_text(prompt)
    return DoctorPhoneResponse(response=response)


@app.get("/health",
         summary="Health Check",
         description="Check if the API is running and healthy",
         response_description="Health status of the API",
         tags=["System"])
def health_check() -> Dict[str, str]:
    """
    Health check endpoint for monitoring and load balancers.
    
    Returns the current health status of the API service.
    """
    return {"status": "healthy"} 



def test_get_hospital_details(hospital_id: int = 1):
    """Test the /hospital/{hospital_id} endpoint locally."""
    import requests
    url = f"http://127.0.0.1:8000/hospital/{hospital_id}"
    try:
        response = requests.get(url)
        print(f"Status code: {response.status_code}")
        print("Response JSON:")
        print(response.json())
    except Exception as e:
        print(f"Error testing endpoint: {e}")

def test_get_user_details(user_id: int = 1):
    """Test the /user/{user_id} endpoint locally."""
    import requests
    url = f"http://127.0.0.1:8000/user/{user_id}"
    try:
        response = requests.get(url)
        print(f"Status code: {response.status_code}")
        print("Response JSON:")
        print(response.json())
    except Exception as e:
        print(f"Error testing endpoint: {e}")

def test_watsonx_functionality():
    """Test the /call_watsonx endpoint locally."""
    import requests
    url = "http://127.0.0.1:8000/call_watsonx"
    try:
        response = requests.post(url)
        print(f"Status code: {response.status_code}")
        print("Response JSON:")
        print(response.json())
    except Exception as e:
        print(f"Error testing endpoint: {e}")
