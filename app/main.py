"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file
  1. Configuration and database connection
  2. Time helpers (epoch ms <-> datetime, IST, shift windows)
  3. Business-rule helpers (R1 - R5, R8)
  4. Serialisation (MongoDB document -> API JSON)
  5. Request models (validation)
  6. Index creation at startup
  7. Endpoints: health, employees, attendance
  8. Analytics: query/pipeline builders + endpoints
  9. Admin: explain
"""
import calendar
import json
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# --------------------------------------------------------------------------- #
# 1. Configuration and database connection
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # .env only fills variables that are not already set

client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,                   # datetimes come back with tzinfo=UTC, never naive
    serverSelectionTimeoutMS=3000,   # fail fast (health -> 503) instead of hanging 30 s
)
db = client[os.getenv("MONGO_DB", "attendance_db")]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
NON_PRESENCE = ("ABSENT", "LEAVE")
GRACE_SECONDS = 10 * 60
OVERTIME_MIN_MINUTES = 30
HALF_DAY_BELOW = Decimal("4.50")
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92

HHMM = r"^([01]\d|2[0-3]):[0-5]\d$"
MONTH = r"^\d{4}-(0[1-9]|1[0-2])$"

# An instant in the API: integer epoch milliseconds. strict=True rejects floats, strings and
# booleans; the lower bound rejects values that look like seconds.
EpochMs = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]


def unprocessable(msg: str, field: str = "body") -> HTTPException:
    """422 in the same shape FastAPI uses for its own validation errors."""
    return HTTPException(422, detail=[{"loc": [field], "msg": msg, "type": "value_error"}])


# --------------------------------------------------------------------------- #
# 2. Time helpers
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """Aware (or naive-UTC) datetime -> epoch ms. None stays None."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def hhmm_to_time(value: str) -> time:
    h, m = map(int, value.split(":"))
    return time(h, m)


def is_overnight(emp: dict) -> bool:
    # "HH:MM" strings are zero-padded, so string comparison is time comparison
    return emp["shift_end"] <= emp["shift_start"]


def attendance_date(punch_in: datetime, emp: dict) -> str:
    """R1: IST calendar date of the punch-in; for overnight shifts, a punch-in earlier
    than shift_end belongs to the shift that started the previous day."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(emp) and local.time() < hhmm_to_time(emp["shift_end"]):
        day -= timedelta(days=1)
    return day.isoformat()


def shift_start_at(date_str: str, emp: dict) -> datetime:
    return datetime.combine(date.fromisoformat(date_str), hhmm_to_time(emp["shift_start"]), IST)


def shift_end_at(date_str: str, emp: dict) -> datetime:
    end = datetime.combine(date.fromisoformat(date_str), hhmm_to_time(emp["shift_end"]), IST)
    if is_overnight(emp):
        end += timedelta(days=1)  # an overnight shift ends on the next calendar day
    return end


# --------------------------------------------------------------------------- #
# 3. Business-rule helpers
# --------------------------------------------------------------------------- #
def half_up(value: Optional[float], places: int) -> Optional[float]:
    """R8: round half-up (not banker's, not float rounding)."""
    if value is None:
        return None
    q = Decimal(1).scaleb(-places)  # 0.01 or 0.0001
    return float(Decimal(repr(value)).quantize(q, rounding=ROUND_HALF_UP))


def compute_late_minutes(punch_in: datetime, date_str: str, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then whole minutes
    since shift start (not since the grace period ended)."""
    seconds = int((punch_in - shift_start_at(date_str, emp)).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: (out - in) seconds / 3600, 2 decimals half-up."""
    seconds = int((punch_out - punch_in).total_seconds())
    hours = (Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(hours)


def compute_overtime(punch_out: datetime, date_str: str, emp: dict) -> int:
    """R3: whole minutes from shift end to punch-out, only when >= 30."""
    seconds = int((punch_out - shift_end_at(date_str, emp)).total_seconds())
    minutes = seconds // 60 if seconds > 0 else 0
    return minutes if minutes >= OVERTIME_MIN_MINUTES else 0


def derived_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime],
                   date_str: str, emp: dict) -> dict:
    """The four derived values for a record, per the table in DATA_MODEL.md."""
    if status in NON_PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {"late_minutes": compute_late_minutes(punch_in, date_str, emp),
           "work_hours": None, "overtime_minutes": 0, "half_day": False}
    if punch_out is not None:
        wh = compute_work_hours(punch_in, punch_out)
        out["work_hours"] = wh
        out["overtime_minutes"] = compute_overtime(punch_out, date_str, emp)
        out["half_day"] = Decimal(repr(wh)) < HALF_DAY_BELOW  # R5 uses the rounded value
    return out


# --------------------------------------------------------------------------- #
# 4. Serialisation: MongoDB document -> API JSON (no _id, instants as epoch ms)
# --------------------------------------------------------------------------- #
def _ms_if_dt(value: Any) -> Any:
    return dt_to_ms(value) if isinstance(value, datetime) else value


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def history_out(entry: dict) -> dict:
    return {
        "at": dt_to_ms(entry.get("at")),
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        "changes": {k: {"from": _ms_if_dt(v.get("from")), "to": _ms_if_dt(v.get("to"))}
                    for k, v in (entry.get("changes") or {}).items()},
    }


def record_out(doc: dict) -> dict:
    # Seeded (legacy) records may omit history, half_day, late/overtime: missing = default
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


# --------------------------------------------------------------------------- #
# 5. Request models
# --------------------------------------------------------------------------- #
class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP\d{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=HHMM)
    shift_end: str = Field("18:30", pattern=HHMM)
    joined_on: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        try:
            return date.fromisoformat(v).isoformat()
        except ValueError:
            raise ValueError("joined_on must be a real date in YYYY-MM-DD format")

    @model_validator(mode="after")
    def _shift_not_empty(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    model_config = ConfigDict(extra="ignore")  # derived fields sent by a client are ignored
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        # The schema does not allow null for these; omitted is fine, explicit null is not
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} may be omitted but not null")
        return self


# --------------------------------------------------------------------------- #
# 6. Indexes (created at startup, idempotent: create_index is a no-op if it exists)
# --------------------------------------------------------------------------- #
def ensure_indexes() -> None:
    emp, logs = db.employees, db.attendance_logs
    # identity + race-safe create (concurrent duplicates -> DuplicateKeyError -> 409)
    emp.create_index([("emp_code", ASCENDING)], unique=True, name="uniq_emp_code")
    # department filter / summary / trend: department, then join date
    emp.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="dept_joined")
    # department summary without a department filter: joined_on <= end of month
    emp.create_index([("joined_on", ASCENDING)], name="joined_on")
    # one record per employee per day + race-safe punch-in; also serves PATCH lookups,
    # per-employee monthly ranges and the $lookup from employees
    logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uniq_emp_date")
    # attendance list sort (date desc, emp_code asc) and date-range analytics
    logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp")
    # punch-out: latest record of an employee with punch_in <= t
    logs.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name="emp_punch_in")


_indexes_ready = False


def try_ensure_indexes() -> None:
    global _indexes_ready
    try:
        ensure_indexes()
        _indexes_ready = True
    except PyMongoError as exc:  # keep the app up so /health can report 503
        print(f"WARNING: could not create indexes: {exc}")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try_ensure_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 7. Endpoints: health, employees, attendance
# --------------------------------------------------------------------------- #
Page = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]


@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "MongoDB is not reachable")
    if not _indexes_ready:  # MongoDB was down at startup: create the indexes now
        try_ensure_indexes()
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        db.employees.insert_one(doc)  # the unique index decides; no check-then-insert
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: Page = 1, page_size: PageSize = 20):
    q = {"department": department} if department is not None else {}
    total = db.employees.count_documents(q)
    cursor = (db.employees.find(q).sort("emp_code", ASCENDING)
              .skip((page - 1) * page_size).limit(page_size))
    return {"items": [employee_out(d) for d in cursor], "total": total,
            "page": page, "page_size": page_size}


def get_employee_or_404(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derived_fields(body.status, ts, None, day, emp),
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)  # unique (emp_code, date): one winner, rest 409
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$ne": None, "$lte": ts}},
        sort=[("punch_in", DESCENDING)],
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    if ts <= rec["punch_in"]:
        raise unprocessable("punched_at must be after punch_in", "punched_at")
    if ts - rec["punch_in"] > MAX_SHIFT:
        raise unprocessable("punched_at is more than 24 hours after punch_in", "punched_at")

    fields = derived_fields(rec["status"], rec["punch_in"], ts, rec["date"], emp)
    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},  # only an open record can be closed
        {"$set": {"punch_out": ts, **fields}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:  # a parallel punch-out closed it between our read and write
        raise HTTPException(409, "already punched out")
    return record_out(updated)


def attendance_list_query(emp_code, date_from, date_to, status, page, page_size):
    """Shared by GET /attendance and its explain."""
    if date_from and date_to and date_from > date_to:
        raise unprocessable("date_from must be <= date_to", "date_from")
    q: dict = {}
    if emp_code:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from.isoformat()
        if date_to:
            q["date"]["$lte"] = date_to.isoformat()
    if status:
        q["status"] = status
    sort = [("date", DESCENDING), ("emp_code", ASCENDING)]
    return q, sort, (page - 1) * page_size, page_size


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    status: Optional[Status] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    q, sort, skip, limit = attendance_list_query(emp_code, date_from, date_to, status, page, page_size)
    total = db.attendance_logs.count_documents(q)
    cursor = db.attendance_logs.find(q).sort(sort).skip(skip).limit(limit)
    return {"items": [record_out(d) for d in cursor], "total": total,
            "page": page, "page_size": page_size}


TRACKED = ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day")


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: date, body: RegularizeIn):  # noqa: A002 (path name from contract)
    day = date.isoformat()
    emp = get_employee_or_404(emp_code)
    rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": day})
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")

    status = body.status or rec["status"]
    if status in NON_PRESENCE:
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable(f"{status} clears punch times; do not send punch_in/punch_out")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else rec.get("punch_in")
        new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else rec.get("punch_out")
        if new_in is None:
            raise unprocessable(f"status {status} requires a punch_in", "punch_in")
        if attendance_date(new_in, emp) != day:
            raise unprocessable("punch_in must stay on the record's attendance date", "punch_in")
        if new_out is not None and not (new_in < new_out <= new_in + MAX_SHIFT):
            raise unprocessable("punch_out must be after punch_in and within 24 hours", "punch_out")

    new = {"status": status, "punch_in": new_in, "punch_out": new_out,
           **derived_fields(status, new_in, new_out, day, emp)}
    old = {"status": rec["status"], "punch_in": rec.get("punch_in"), "punch_out": rec.get("punch_out"),
           "work_hours": rec.get("work_hours"), "late_minutes": rec.get("late_minutes") or 0,
           "overtime_minutes": rec.get("overtime_minutes") or 0, "half_day": bool(rec.get("half_day", False))}
    changes = {k: {"from": old[k], "to": new[k]} for k in TRACKED if old[k] != new[k]}
    if not changes:
        raise unprocessable("the request changes nothing")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    # Optimistic concurrency: only write if nobody changed the record since we read it.
    # history only grows, so its length works as a version number.
    guard = {
        "_id": rec["_id"],
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(rec.get("history") or [])]},
    }
    updated = db.attendance_logs.find_one_and_update(
        guard, {"$set": new, "$push": {"history": entry}}, return_document=ReturnDocument.AFTER
    )
    if updated is None:
        raise HTTPException(409, "record was modified concurrently; retry")
    return record_out(updated)


# --------------------------------------------------------------------------- #
# 8. Analytics
# --------------------------------------------------------------------------- #
MonthParam = Annotated[str, Query(pattern=MONTH)]


def month_bounds(month: str) -> tuple[date, date]:
    y, m = map(int, month.split("-"))
    return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])


def count_weekdays(start: date, end: date) -> int:
    """Mon-Fri days in [start, end] (R7). Plain calendar arithmetic, no data involved."""
    if start > end:
        return 0
    days = (end - start).days + 1
    full_weeks, rest = divmod(days, 7)
    extra = sum(1 for i in range(rest) if (start + timedelta(days=full_weeks * 7 + i)).weekday() < 5)
    return full_weeks * 5 + extra


# Aggregation expressions reused by several pipelines ------------------------
def _iso_dow(date_field: str) -> dict:
    """1 = Monday ... 7 = Sunday, from a "YYYY-MM-DD" string field."""
    return {"$isoDayOfWeek": {"$dateFromString": {"dateString": date_field, "format": "%Y-%m-%d"}}}


def _present_value(prefix: str = "$") -> dict:
    """R5/R6: 1 for a full present day, 0.5 for a half day, 0 otherwise."""
    return {"$cond": [{"$in": [f"{prefix}status", list(PRESENCE)]},
                      {"$cond": [{"$eq": [{"$ifNull": [f"{prefix}half_day", False]}, True]}, 0.5, 1]},
                      0]}


def _present_weekday_value(prefix: str = "$") -> dict:
    """R7: present_days only counts Mon-Fri records."""
    return {"$cond": [{"$lte": [_iso_dow(f"{prefix}date"), 5]}, _present_value(prefix), 0]}


def _late(prefix: str = "$") -> dict:
    return {"$ifNull": [f"{prefix}late_minutes", 0]}


def employee_monthly_pipeline(emp_code: str, start: date, end: date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": _present_weekday_value()},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [_late(), 0]}, 1, 0]}},
            "total_late_minutes": {"$sum": _late()},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthParam):
    emp = get_employee_or_404(emp_code)
    start, end = month_bounds(month)
    joined = date.fromisoformat(emp["joined_on"])
    working_days = count_weekdays(max(start, joined), end)

    rows = list(db.attendance_logs.aggregate(employee_monthly_pipeline(emp_code, start, end)))
    agg = rows[0] if rows else {}
    present = float(agg.get("present_days", 0))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present,
        "leave_days": int(agg.get("leave_days", 0)),
        "late_count": int(agg.get("late_count", 0)),
        "total_late_minutes": int(agg.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(agg.get("total_overtime_minutes", 0)),
        "attendance_pct": half_up(present / working_days * 100, 2) if working_days else None,
    }


def department_summary_pipeline(start: date, end: date, department: Optional[str]) -> list:
    """Runs on employees, so people with zero logs are still in the headcount (R9)."""
    match: dict = {"joined_on": {"$lte": end.isoformat()}}
    if department is not None:
        match["department"] = department
    is_present = {"$in": ["$logs.status", list(PRESENCE)]}
    has_hours = {"$and": [is_present, {"$isNumber": "$logs.work_hours"}]}
    return [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1,
                              "late_minutes": 1, "work_hours": 1}},
            ],
            "as": "logs",
        }},
        # one row per log; employees without logs keep one row with logs = missing
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": "$department",
            "employees": {"$addToSet": "$emp_code"},
            "present_days": {"$sum": {"$cond": [{"$ifNull": ["$logs.date", False]},
                                                _present_weekday_value("$logs."), 0]}},
            # sum work hours as integer hundredths so the average is exact before rounding
            "wh_cents": {"$sum": {"$cond": [has_hours, {"$round": [{"$multiply": ["$logs.work_hours", 100]}, 0]}, 0]}},
            "wh_count": {"$sum": {"$cond": [has_hours, 1, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [_late("$logs."), 0]}, 1, 0]}},
            "total_late_minutes": {"$sum": _late("$logs.")},
            "leave_count": {"$sum": {"$cond": [{"$eq": ["$logs.status", "LEAVE"]}, 1, 0]}},
            "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$logs.status", "ON_DUTY"]}, 1, 0]}},
        }},
        {"$project": {"_id": 0, "department": "$_id", "headcount": {"$size": "$employees"},
                      "present_days": 1, "wh_cents": 1, "wh_count": 1, "late_count": 1,
                      "total_late_minutes": 1, "leave_count": 1, "on_duty_count": 1}},
        {"$match": {"headcount": {"$gt": 0}}},
        {"$sort": {"department": 1}},
    ]


@app.get("/analytics/departments/summary")
def department_summary(month: MonthParam, department: Optional[str] = None):
    start, end = month_bounds(month)
    items = []
    for row in db.employees.aggregate(department_summary_pipeline(start, end, department)):
        avg = None
        if row["wh_count"]:
            avg = float((Decimal(int(row["wh_cents"])) / Decimal(100) / Decimal(row["wh_count"]))
                        .quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        items.append({
            "department": row["department"],
            "headcount": int(row["headcount"]),
            "present_days": float(row["present_days"]),
            "avg_work_hours": avg,
            "late_count": int(row["late_count"]),
            "total_late_minutes": int(row["total_late_minutes"]),
            "leave_count": int(row["leave_count"]),
            "on_duty_count": int(row["on_duty_count"]),
        })
    return {"month": month, "items": items}


def late_leaderboard_pipeline(start: date, end: date, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": {"date": {"$gte": start.isoformat(), "$lte": end.isoformat()},
                    "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"},
                    "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs whose employee does not exist are dropped here
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department
    pipeline += [
        # standard competition ranking: ties share a rank, the next rank is skipped (1, 2, 2, 4)
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1},
                              "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies AFTER ranking
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name",
                      "department": "$emp.department", "total_late_minutes": 1, "late_count": 1}},
    ]
    return pipeline


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthParam, limit: Annotated[int, Query(ge=1, le=50)] = 10,
                     department: Optional[str] = None):
    start, end = month_bounds(month)
    items = [
        {"rank": int(r["rank"]), "emp_code": r["emp_code"], "name": r["name"],
         "department": r["department"], "total_late_minutes": int(r["total_late_minutes"]),
         "late_count": int(r["late_count"])}
        for r in db.attendance_logs.aggregate(late_leaderboard_pipeline(start, end, limit, department))
    ]
    return {"month": month, "items": items}


def department_trend_pipeline(department: str, start: date, end: date) -> list:
    """Runs on employees of the department. Each employee contributes a 'join' event on the
    first day it counts towards headcount, plus one event per log in the range. Events are
    grouped per day, missing days are filled with $densify, a running sum gives headcount and
    a 7-row window gives the moving average."""
    lo, hi = start.isoformat(), end.isoformat()
    first_counted_day = {"$max": ["$joined_on", lo]}  # joined before the range -> counts from `from`
    return [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": lo, "$lte": hi}}},
                {"$project": {"_id": 0, "d": "$date",
                              "present": _present_value(),
                              "late": {"$cond": [{"$gt": [_late(), 0]}, 1, 0]}}},
            ],
            "as": "logs",
        }},
        {"$project": {"_id": 0, "events": {"$concatArrays": [
            {"$cond": [{"$lte": ["$joined_on", hi]},
                       [{"d": first_counted_day, "join": 1, "present": 0, "late": 0}], []]},
            {"$map": {"input": "$logs", "as": "l",
                      "in": {"d": "$$l.d", "join": 0, "present": "$$l.present", "late": "$$l.late"}}},
        ]}}},
        {"$unwind": "$events"},
        {"$replaceRoot": {"newRoot": "$events"}},
        # make sure the series is never empty, even if nobody has joined yet
        {"$unionWith": {"pipeline": [{"$documents": [{"d": lo, "join": 0, "present": 0, "late": 0}]}]}},
        {"$group": {"_id": "$d", "joins": {"$sum": "$join"},
                    "present_count": {"$sum": "$present"}, "late_count": {"$sum": "$late"}}},
        {"$addFields": {"day": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        # gap filling: exactly one document per calendar day in [from, to]
        {"$densify": {"field": "day", "range": {
            "step": 1, "unit": "day",
            "bounds": [datetime.combine(start, time(), UTC),
                       datetime.combine(end + timedelta(days=1), time(), UTC)]}}},
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "headcount": {"$sum": {"$ifNull": ["$joins", 0]},
                          "window": {"documents": ["unbounded", "current"]}}}}},
        {"$addFields": {
            "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
        }},
        {"$addFields": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            {"$divide": ["$present_count", "$headcount"]},
            None]}}},
        # $avg ignores nulls; window = this row and up to 6 rows before it, inside the range
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "moving_avg_7d": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}}}},
        {"$sort": {"day": 1}},
        {"$project": {"_id": 0, "date": 1, "is_working_day": 1, "headcount": 1, "present_count": 1,
                      "late_count": 1, "attendance_rate": 1, "moving_avg_7d": 1}},
    ]


def validate_trend_range(start: date, end: date) -> None:
    if end < start:
        raise unprocessable("to must be on or after from", "to")
    if (end - start).days + 1 > MAX_TREND_DAYS:
        raise unprocessable(f"range must not exceed {MAX_TREND_DAYS} days", "to")


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str,
                     start: Annotated[date, Query(alias="from")],
                     end: Annotated[date, Query(alias="to")]):
    validate_trend_range(start, end)
    if db.employees.count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(404, "unknown department")
    items = [
        {"date": r["date"], "is_working_day": bool(r["is_working_day"]),
         "headcount": int(r["headcount"]), "present_count": float(r["present_count"]),
         "late_count": int(r["late_count"]),
         "attendance_rate": half_up(r["attendance_rate"], 4),
         "moving_avg_7d": half_up(r["moving_avg_7d"], 4)}
        for r in db.employees.aggregate(department_trend_pipeline(department, start, end))
    ]
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 9. Admin: explain the main query of an endpoint, built exactly as the endpoint builds it
# --------------------------------------------------------------------------- #
def _require(**params) -> None:
    missing = [k for k, v in params.items() if v is None]
    if missing:
        raise unprocessable(f"missing required parameter(s): {', '.join(missing)}", "query")


def _to_json(doc: dict) -> dict:
    """Explain output contains BSON types (dates, ObjectIds); make it plain JSON."""
    return json.loads(json_util.dumps(doc))


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary",
                      "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=MONTH)] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    status: Optional[Status] = None,
    start: Annotated[Optional[date], Query(alias="from")] = None,
    end: Annotated[Optional[date], Query(alias="to")] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    if endpoint == "attendance_list":
        q, sort, skip, lim = attendance_list_query(emp_code, date_from, date_to, status, page, page_size)
        coll = "attendance_logs"
        command = {"find": coll, "filter": q, "sort": dict(sort), "skip": skip, "limit": lim}
    elif endpoint == "employee_monthly":
        _require(emp_code=emp_code, month=month)
        coll = "attendance_logs"
        command = {"aggregate": coll, "pipeline": employee_monthly_pipeline(emp_code, *month_bounds(month)),
                   "cursor": {}}
    elif endpoint == "department_summary":
        _require(month=month)
        coll = "employees"
        command = {"aggregate": coll, "pipeline": department_summary_pipeline(*month_bounds(month), department),
                   "cursor": {}}
    elif endpoint == "late_leaderboard":
        _require(month=month)
        coll = "attendance_logs"
        command = {"aggregate": coll,
                   "pipeline": late_leaderboard_pipeline(*month_bounds(month), limit, department),
                   "cursor": {}}
    else:  # department_trend
        _require(department=department, **{"from": start, "to": end})
        validate_trend_range(start, end)
        coll = "employees"
        command = {"aggregate": coll, "pipeline": department_trend_pipeline(department, start, end),
                   "cursor": {}}
    result = db.command({"explain": command, "verbosity": "executionStats"})
    return {"endpoint": endpoint, "collection": coll, "explain": _to_json(result)}
