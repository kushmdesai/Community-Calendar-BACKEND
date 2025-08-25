from fastapi import FastAPI, HTTPException, Depends, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Optional
import sqlite3
from contextlib import contextmanager
import os
import uvicorn
from enum import Enum

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
            is_recurring BOOLEAN DEFAULT FALSE,
            recurrence_type TEXT,
            recurrence_interval INTEGER DEFAULT 1,
            recurrence_end_date DATE,
            parent_event_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_event_id) REFERENCES events (id)
        )
    """)
    
    # Add columns if they don't exist (for existing databases)
    try:
        cursor.execute("ALTER TABLE events ADD COLUMN is_recurring BOOLEAN DEFAULT FALSE")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE events ADD COLUMN recurrence_type TEXT")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE events ADD COLUMN recurrence_interval INTEGER DEFAULT 1")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE events ADD COLUMN recurrence_end_date DATE")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE events ADD COLUMN parent_event_id INTEGER")
    except sqlite3.OperationalError:
        pass

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

class RecurrenceType(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"

# Pydantic models for request/response validation
class EventCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200, description="Event title")
    description: Optional[str] = Field(None, max_length=1000, description="Event description")
    event_date: date = Field(..., description="Event date (YYYY-MM-DD)")
    event_time: Optional[str] = Field(None, pattern=r"^([01]?[0-9]|2[0-3]):[0-5][0-9]$", description="Event time (HH:MM)")
    organizer: Optional[str] = Field(None, max_length=100, description="Event organizer")
    is_recurring: Optional[bool] = Field(False, description="Is this a recurring event")
    recurrence_type: Optional[RecurrenceType] = Field(None, description="Type of recurrence")
    recurrence_interval: Optional[int] = Field(1, ge=1, le=365, description="Recurrence interval")
    recurrence_end_date: Optional[date] = Field(None, description="When to stop recurrence")

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
    is_recurring: Optional[bool] = None
    recurrence_type: Optional[RecurrenceType] = None
    recurrence_interval: Optional[int] = Field(None, ge=1, le=365)
    recurrence_end_date: Optional[date] = None

class EventResponse(BaseModel):
    id: int
    title: str
    description: Optional[str]
    event_date: date
    event_time: Optional[str]
    organizer: Optional[str]
    is_recurring: Optional[bool]
    recurrence_type: Optional[str]
    recurrence_interval: Optional[int]
    recurrence_end_date: Optional[date]
    parent_event_id: Optional[int]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

def generate_recurring_events(base_event: dict) -> List[dict]:
    """Generate recurring events based on the base event"""
    if not base_event.get('is_recurring') or not base_event.get('recurrence_type'):
        return []
    
    events = []
    current_date = base_event['event_date']
    end_date = base_event.get('recurrence_end_date')
    interval = base_event.get('recurrence_interval', 1)
    recurrence_type = base_event['recurrence_type']

    # Set a reasonable end date if none provided (2 years max)
    max_date = end_date if end_date else (current_date + relativedelta(years=2))

    iteration_count = 0
    while current_date <= max_date and iteration_count < 1000:  # Safety limit
        if current_date > base_event['event_date']:  # Don't include the original event
            recurring_event = base_event.copy()
            recurring_event['event_date'] = current_date
            recurring_event['parent_event_id'] = base_event['id']
            events.append(recurring_event)

        # Calculate next occurrence
        if recurrence_type == 'daily':
            current_date += timedelta(days=interval)
        elif recurrence_type == 'weekly':
            current_date += timedelta(weeks=interval)
        elif recurrence_type == 'monthly':
            current_date += relativedelta(months=interval)
        elif recurrence_type == 'yearly':
            current_date += relativedelta(years=interval)
        
        iteration_count += 1

    return events

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
    print(f"Creating event with data: {event}")
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            print(f"Inserting: title={event.title}, date={event.event_date}, time={event.event_time}")
            cursor.execute("""
                INSERT INTO events (title, description, date, time, organizer, is_recurring, recurrence_type, recurrence_interval, recurrence_end_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (event.title, event.description, event.event_date, event.event_time, event.organizer,
                  event.is_recurring, event.recurrence_type, event.recurrence_interval, event.recurrence_end_date))
            
            event_id = cursor.lastrowid
            conn.commit()
            print(f"Event created with ID: {event_id}")
            
            # Generate recurring events if needed
            if event.is_recurring:
                base_event_dict = {
                    'id': event_id,
                    'title': event.title,
                    'description': event.description,
                    'event_date': event.event_date,
                    'event_time': event.event_time,
                    'organizer': event.organizer,
                    'is_recurring': event.is_recurring,
                    'recurrence_type': event.recurrence_type,
                    'recurrence_interval': event.recurrence_interval,
                    'recurrence_end_date': event.recurrence_end_date
                }

                recurring_events = generate_recurring_events(base_event_dict)

                for rec_event in recurring_events:
                    cursor.execute("""
                        INSERT INTO events (title, description, date, time, organizer, is_recurring, recurrence_type, recurrence_interval, recurrence_end_date, parent_event_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (rec_event['title'], rec_event['description'], rec_event['event_date'],
                          rec_event['event_time'], rec_event['organizer'], False, None, None, None, rec_event['parent_event_id']))
                    
                conn.commit()

            # Fetch the created event
            cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            row = cursor.fetchone()
            
            if row:
                # Helper function to safely get column values
                def safe_get(row, column, default=None):
                    try:
                        return row[column] if row[column] is not None else default
                    except (KeyError, IndexError):
                        return default
                
                return EventResponse(
                    id=row["id"],
                    title=row["title"],
                    description=row["description"],
                    event_date=row["date"],
                    event_time=row["time"],
                    organizer=row["organizer"],
                    is_recurring=bool(safe_get(row, 'is_recurring', False)),
                    recurrence_type=safe_get(row, 'recurrence_type'),
                    recurrence_interval=safe_get(row, 'recurrence_interval'),
                    recurrence_end_date=safe_get(row, 'recurrence_end_date'),
                    parent_event_id=safe_get(row, 'parent_event_id'),
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"])
                )
    
    except Exception as e:
        print(f"Error creating event: {str(e)}")
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
            
            # Helper function to safely get column values
            def safe_get(row, column, default=None):
                try:
                    return row[column] if row[column] is not None else default
                except (KeyError, IndexError):
                    return default
            
            events = []
            for row in rows:
                events.append(EventResponse(
                    id=row["id"],
                    title=row["title"],
                    description=row["description"],
                    event_date=row["date"],
                    event_time=row["time"],
                    organizer=row["organizer"],
                    is_recurring=bool(safe_get(row, 'is_recurring', False)),
                    recurrence_type=safe_get(row, 'recurrence_type'),
                    recurrence_interval=safe_get(row, 'recurrence_interval'),
                    recurrence_end_date=safe_get(row, 'recurrence_end_date'),
                    parent_event_id=safe_get(row, 'parent_event_id'),
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"])
                ))
            
            return events
    
    except Exception as e:
        print(f"Error fetching events: {str(e)}")
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
            
            # Helper function to safely get column values
            def safe_get(row, column, default=None):
                try:
                    return row[column] if row[column] is not None else default
                except (KeyError, IndexError):
                    return default
            
            return EventResponse(
                id=row["id"],
                title=row["title"],
                description=row["description"],
                event_date=row["date"],
                event_time=row["time"],
                organizer=row["organizer"],
                is_recurring=bool(safe_get(row, 'is_recurring', False)),
                recurrence_type=safe_get(row, 'recurrence_type'),
                recurrence_interval=safe_get(row, 'recurrence_interval'),
                recurrence_end_date=safe_get(row, 'recurrence_end_date'),
                parent_event_id=safe_get(row, 'parent_event_id'),
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"])
            )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching event: {str(e)}")
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

            if event.is_recurring is not None:
                update_fields.append("is_recurring = ?")
                params.append(event.is_recurring)

            if event.recurrence_type is not None:
                update_fields.append("recurrence_type = ?")
                params.append(event.recurrence_type)

            if event.recurrence_interval is not None:
                update_fields.append("recurrence_interval = ?")
                params.append(event.recurrence_interval)

            if event.recurrence_end_date is not None:
                update_fields.append("recurrence_end_date = ?")
                params.append(event.recurrence_end_date)
            
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
            
            # Helper function to safely get column values
            def safe_get(row, column, default=None):
                try:
                    return row[column] if row[column] is not None else default
                except (KeyError, IndexError):
                    return default
            
            return EventResponse(
                id=row["id"],
                title=row["title"],
                description=row["description"],
                event_date=row["date"],
                event_time=row["time"],
                organizer=row["organizer"],
                is_recurring=bool(safe_get(row, 'is_recurring', False)),
                recurrence_type=safe_get(row, 'recurrence_type'),
                recurrence_interval=safe_get(row, 'recurrence_interval'),
                recurrence_end_date=safe_get(row, 'recurrence_end_date'),
                parent_event_id=safe_get(row, 'parent_event_id'),
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"])
            )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating event: {str(e)}")
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
            
            # Delete the event and any recurring instances
            cursor.execute("DELETE FROM events WHERE id = ? OR parent_event_id = ?", (event_id, event_id))
            conn.commit()
            
            return {"message": "Event deleted successfully", "event_id": event_id}
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting event: {str(e)}")
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
            
            # Helper function to safely get column values
            def safe_get(row, column, default=None):
                try:
                    return row[column] if row[column] is not None else default
                except (KeyError, IndexError):
                    return default
            
            events = []
            for row in rows:
                events.append(EventResponse(
                    id=row["id"],
                    title=row["title"],
                    description=row["description"],
                    event_date=row["date"],
                    event_time=row["time"],
                    organizer=row["organizer"],
                    is_recurring=bool(safe_get(row, 'is_recurring', False)),
                    recurrence_type=safe_get(row, 'recurrence_type'),
                    recurrence_interval=safe_get(row, 'recurrence_interval'),
                    recurrence_end_date=safe_get(row, 'recurrence_end_date'),
                    parent_event_id=safe_get(row, 'parent_event_id'),
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"])
                ))
            
            return events
    
    except Exception as e:
        print(f"Error fetching events by date: {str(e)}")
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
        print(f"Error fetching stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch statistics: {str(e)}")

@app.post("/api/events/debug", tags=["Debug"])
async def debug_create_event(request: dict):
    """Debug endpoint to see what data is being sent"""
    print("Received data:", request)
    
    # Try to validate against EventCreate
    try:
        event = EventCreate(**request)
        print("Validation successful:", event)
        return {"status": "valid", "parsed_data": event.dict()}
    except Exception as e:
        print("Validation error:", str(e))
        return {"status": "invalid", "error": str(e), "received": request}

@app.get("/api/calendar/export.ics", tags=["Export"])
async def export_calendar():
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM events ORDER BY date ASC, time ASC")
            rows = cursor.fetchall()

            if not rows:
                raise HTTPException(status_code=404, detail="No events found to export")
            
            ics = "BEGIN:VCALENDAR\nVERSION:2.0\nPROID:-CommunityCalendar//EN\n"

            for row in rows:

                event_date = datetime.fromisoformat(row["date"])
                if row["time"]:
                    hours, minutes = map(int, row["time"].split(":"))
                    event_start = event_date.replace(hour=hours, minute=minutes)
                else:
                    event_start =  event_date.replace(hour=0, minute=0)

                event_end = event_start + timedelta(hours=1)

                dtstamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                dtstart = event_start.strftime("%Y%m%dT%H%M%SZ")
                dtend = event_end.strftime("%Y%m%dT%H%M%SZ")

                ics += "BEGIN:VEVENT\n"
                ics += f"UID:{row['id']}@communitycalendar\n"
                ics += f"DTSTAMP:{dtstamp}\n"
                ics += f"DTSTART:{dtstart}\n"
                ics += f"DTEND:{dtend}\n"
                ics += f"SUMMARY:{row['title']}\n"
                if row["description"]:
                    ics += f"DESCRIPTION:{row['description']}\n"
                if row["organizer"]:
                    ics += f"ORGANIZER:{row['organizer']}\n"

                # Handle recurrence if enabled
                if row["is_recurring"] and row["recurrence_type"]:
                    freq = row["recurrence_type"].upper()
                    interval = row["recurrence_interval"] or 1
                    rrule = f"FREQ={freq};INTERVAL={interval}"

                    if row["recurrence_end_date"]:
                        until = datetime.fromisoformat(row["recurrence_end_date"]).strftime("%Y%m%dT%H%M%SZ")
                        rrule += f";UNTIL={until}"

                    ics += f"RRULE:{rrule}\n"
                ics += f"END:VEVENT\n"
            ics += "END:VCALENDAR"
            return Response(
                content=ics,
                media_type="text/calendar",
                headers={"Content-Disposition": "attachment; filename=calendar.ics"}
            )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error exporting as ics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to export calendar: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)