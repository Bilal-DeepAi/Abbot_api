from fastapi import FastAPI
from pydantic import BaseModel
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime
from collections import defaultdict

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI(title="Abbott Test Priority Day & Hour API")

class DateRange(BaseModel):
    start_date: str
    end_date: str
@app.post("/priority-stats")
def testpriority_day_hour(payload: DateRange):

    response = supabase.table("abbott_test_data") \
        .select("testpriority, samplearrivaldatetime, testdepartment, samplefluidtype") \
        .gte("samplearrivaldatetime", f"{payload.start_date}T00:00:00Z") \
        .lte("samplearrivaldatetime", f"{payload.end_date}T23:59:59Z") \
        .execute()

    rows = response.data or []
    
    per_day = defaultdict(lambda: {
        "Routine": 0,
        "Stat": 0
    })

    per_day_hour = defaultdict(
        lambda: {
            f"{h:02d}": {
                "Routine": 0,
                "Stat": 0
            } for h in range(24)
        }
    )
    
    per_day_department = defaultdict(lambda: defaultdict(int))
    per_day_hour_department = defaultdict(
        lambda: {f"{h:02d}": defaultdict(int) for h in range(24)}
    )
    per_day_samplefluidtype = defaultdict(lambda: defaultdict(int))
    per_day_hour_samplefluidtype = defaultdict(
        lambda: {f"{h:02d}": defaultdict(int) for h in range(24)}
    )

    for row in rows:
        try:
            priority = row["testpriority"]
            department = row.get("testdepartment")
            sample_fluid_type = row.get("samplefluidtype")
            dt = datetime.fromisoformat(
                row["samplearrivaldatetime"].replace("Z", "")
            )
            date_key = dt.date().isoformat()
            hour_key = f"{dt.hour:02d}"
           
            if priority in ["Routine", "Stat"]:
                per_day[date_key][priority] += 1
                per_day_hour[date_key][hour_key][priority] += 1
           
            if department:
                per_day_department[date_key][department] += 1
                per_day_hour_department[date_key][hour_key][department] += 1

            if sample_fluid_type:
                per_day_samplefluidtype[date_key][sample_fluid_type] += 1
                per_day_hour_samplefluidtype[date_key][hour_key][sample_fluid_type] += 1

        except Exception:
            continue

    return {
        "status": "success",
        "start_date": payload.start_date,
        "end_date": payload.end_date,
        "per_day": dict(per_day),
        "per_day_hourly": dict(per_day_hour),
        "per_day_department": {d: dict(per_day_department[d]) for d in per_day_department},
        "per_day_hourly_department": {d: {h: dict(per_day_hour_department[d][h]) for h in per_day_hour_department[d]}
            for d in per_day_hour_department},
        "per_day_samplefluidtype": {d: dict(per_day_samplefluidtype[d]) for d in per_day_samplefluidtype},
        "per_day_hourly_samplefluidtype": {d: {h: dict(per_day_hour_samplefluidtype[d][h]) for h in per_day_hour_samplefluidtype[d]}
            for d in per_day_hour_samplefluidtype},
    }