from fastapi import FastAPI
from pydantic import BaseModel
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(title="Abbott Average Samples & Patients API")

class DateRange(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD

@app.post("/overall-average")
def average_samples_patients(payload: DateRange):
    start_date = payload.start_date
    end_date = payload.end_date

    # Fetch date & time data from Supabase
    response = supabase.table("abbott_test_data") \
        .select("collectiondatetime, patientid_requisitionid") \
        .gte("collectiondatetime", f"{start_date}T00:00:00Z") \
        .lte("collectiondatetime", f"{end_date}T23:59:59Z") \
        .execute()

    rows = response.data

    # Track unique dates & patients
    unique_dates = set()
    unique_patients = set()

    for row in rows:
        try:
            date_only = datetime.fromisoformat(
                row["collectiondatetime"].replace("Z", "")
            ).date()
            unique_dates.add(date_only)
            unique_patients.add(row["patientid_requisitionid"])
        except Exception:
            continue

    total_samples = len(rows) 
    total_unique_dates = len(unique_dates)
    total_unique_patients = len(unique_patients)
    total_test_departments = len(rows)   
    average_samples_per_day = (total_samples / total_unique_dates) if total_unique_dates > 0 else 0
    average_patients_per_day = (total_unique_patients / total_unique_dates) if total_unique_dates > 0 else 0
    average_tests_per_day = (total_test_departments / total_unique_dates) if total_unique_dates > 0 else 0
    return {
        "status": "success",
        "start_date": start_date,
        "end_date": end_date,
        "total_samples": total_samples,
        "total_unique_patients": total_unique_patients,
        "unique_dates_count": total_unique_dates,
        "total_test_departments": total_test_departments,
        "average_samples_per_day": round(average_samples_per_day, 2),
        "average_patients_per_day": round(average_patients_per_day, 2),
        "average_tests_per_day": round(average_tests_per_day, 2)
    }
