from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import List, Optional
import sqlite3
from contextlib import contextmanager
import os
import uvicorn

# Initialize FastAPI app
app = FastAPI(
    title="Community Calendar API",
    description="A REST API for managing community events",
    version="1.0.0"
)

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database setup
DATABASE_URL = "calendar.db"

def init_db():
    """Initialize the database with events table"""
    conn = sqlite3.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            date DATE NOT NULL,
            time TEXT,
            organizer TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()

@contextmanager
def get_db():
    """Database context manager"""
    conn = sqlite3.connect(DATABASE_URL)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    try:
        yield conn
    finally:
        conn.close()

# Pydantic models for request/response validation
class EventCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200, description="Event title")
    description: Optional[str] = Field(None, max_length=1000, description="Event description")
    event_date: date = Field(..., description="Event date (YYYY-MM-DD)")
    event_time: Optional[str] = Field(None, pattern=r"^([01]?[0-9]|2[0-3]):[0-5][0-9]$", description="Event time (HH:MM)")
    organizer: Optional[str] = Field(None, max_length=100, description="Event organizer")

    model_config = {
        "json_schema_extra": {
            "example": {
                "title": "Community BBQ",
                "description": "Join us for a fun community barbecue in the park!",
                "event_date": "2024-07-15",
                "event_time": "18:00",
                "organizer": "Community Center"
            }
        }
    }

class EventUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    event_date: Optional[date] = None
    event_time: Optional[str] = Field(None, pattern=r"^([01]?[0-9]|2[0-3]):[0-5][0-9]$")
    organizer: Optional[str] = Field(None, max_length=100)

class EventResponse(BaseModel):
    id: int
    title: str
    description: Optional[str]
    event_date: date
    event_time: Optional[str]
    organizer: Optional[str]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

# API Routes

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    init_db()

@app.get("/", tags=["Root"])
async def root():
    """Welcome message"""
    return {"message": "Community Calendar API", "version": "1.0.0"}

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now()}

@app.post("/api/events", response_model=EventResponse, tags=["Events"])
async def create_event(event: EventCreate):
    """Create a new event"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO events (title, description, date, time, organizer)
                VALUES (?, ?, ?, ?, ?)
            """, (event.title, event.description, event.event_date, event.event_time, event.organizer))
            
            event_id = cursor.lastrowid
            conn.commit()
            
            # Fetch the created event
            cursor.execute("""
                SELECT * FROM events WHERE id = ?
            """, (event_id,))
            
            row = cursor.fetchone()
            if row:
                return EventResponse(
                    id=row["id"],
                    title=row["title"],
                    description=row["description"],
                    event_date=row["date"],
                    event_time=row["time"],
                    organizer=row["organizer"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"])
                )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create event: {str(e)}")

@app.get("/api/events", response_model=List[EventResponse], tags=["Events"])
async def get_all_events(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    organizer: Optional[str] = None
):
    """Get all events with optional filtering"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM events WHERE 1=1"
            params = []
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            
            if organizer:
                query += " AND organizer LIKE ?"
                params.append(f"%{organizer}%")
            
            query += " ORDER BY date ASC, time ASC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            events = []
            for row in rows:
                events.append(EventResponse(
                    id=row["id"],
                    title=row["title"],
                    description=row["description"],
                    event_date=row["date"],
                    event_time=row["time"],
                    organizer=row["organizer"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"])
                ))
            
            return events
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch events: {str(e)}")

@app.get("/api/events/{event_id}", response_model=EventResponse, tags=["Events"])
async def get_event(event_id: int):
    """Get a specific event by ID"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            row = cursor.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Event not found")
            
            return EventResponse(
                id=row["id"],
                title=row["title"],
                description=row["description"],
                event_date=row["date"],
                event_time=row["time"],
                organizer=row["organizer"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"])
            )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch event: {str(e)}")

@app.put("/api/events/{event_id}", response_model=EventResponse, tags=["Events"])
async def update_event(event_id: int, event: EventUpdate):
    """Update an existing event"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Check if event exists
            cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Event not found")
            
            # Build update query dynamically
            update_fields = []
            params = []
            
            if event.title is not None:
                update_fields.append("title = ?")
                params.append(event.title)
            
            if event.description is not None:
                update_fields.append("description = ?")
                params.append(event.description)
            
            if event.event_date is not None:
                update_fields.append("date = ?")
                params.append(event.event_date)
            
            if event.event_time is not None:
                update_fields.append("time = ?")
                params.append(event.event_time)
            
            if event.organizer is not None:
                update_fields.append("organizer = ?")
                params.append(event.organizer)
            
            if not update_fields:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            params.append(event_id)
            
            query = f"UPDATE events SET {', '.join(update_fields)} WHERE id = ?"
            cursor.execute(query, params)
            conn.commit()
            
            # Fetch updated event
            cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            row = cursor.fetchone()
            
            return EventResponse(
                id=row["id"],
                title=row["title"],
                description=row["description"],
                event_date=row["date"],
                event_time=row["time"],
                organizer=row["organizer"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"])
            )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update event: {str(e)}")

@app.delete("/api/events/{event_id}", tags=["Events"])
async def delete_event(event_id: int):
    """Delete an event"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Event not found")
            
            cursor.execute("DELETE FROM events WHERE id = ?", (event_id,))
            conn.commit()
            
            return {"message": "Event deleted successfully", "event_id": event_id}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete event: {str(e)}")

@app.get("/api/events/date/{event_date}", response_model=List[EventResponse], tags=["Events"])
async def get_events_by_date(event_date: date):
    """Get all events for a specific date"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM events 
                WHERE date = ? 
                ORDER BY time ASC
            """, (event_date,))
            
            rows = cursor.fetchall()
            events = []
            
            for row in rows:
                events.append(EventResponse(
                    id=row["id"],
                    title=row["title"],
                    description=row["description"],
                    event_date=row["date"],
                    event_time=row["time"],
                    organizer=row["organizer"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"])
                ))
            
            return events
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch events: {str(e)}")

# Statistics endpoint
@app.get("/api/stats", tags=["Statistics"])
async def get_stats():
    """Get calendar statistics"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Total events
            cursor.execute("SELECT COUNT(*) FROM events")
            total_events = cursor.fetchone()[0]
            
            # Events this month
            current_date = datetime.now().date()
            first_day = current_date.replace(day=1)
            if current_date.month == 12:
                last_day = current_date.replace(year=current_date.year + 1, month=1, day=1)
            else:
                last_day = current_date.replace(month=current_date.month + 1, day=1)
            
            cursor.execute("SELECT COUNT(*) FROM events WHERE date >= ? AND date < ?", 
                          (first_day, last_day))
            events_this_month = cursor.fetchone()[0]
            
            # Upcoming events (next 30 days)
            from datetime import timedelta
            future_date = current_date + timedelta(days=30)
            cursor.execute("SELECT COUNT(*) FROM events WHERE date >= ? AND date <= ?", 
                          (current_date, future_date))
            upcoming_events = cursor.fetchone()[0]
            
            return {
                "total_events": total_events,
                "events_this_month": events_this_month,
                "upcoming_events": upcoming_events,
                "generated_at": datetime.now()
            }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch statistics: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)