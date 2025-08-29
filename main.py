from fastapi import FastAPI, HTTPException, Depends, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, EmailStr
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
    description="A REST API for managing community events with RSVP and location features",
    version="2.0.0"
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
    """Initialize the database with events and rsvps tables"""
    conn = sqlite3.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Events table with location support
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
            location_type TEXT DEFAULT 'in_person',
            location_name TEXT,
            location_address TEXT,
            online_meeting_url TEXT,
            max_attendees INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_event_id) REFERENCES events (id)
        )
    """)
    
    # RSVPs table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rsvps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER NOT NULL,
            attendee_name TEXT NOT NULL,
            attendee_email TEXT,
            status TEXT NOT NULL DEFAULT 'going',
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (event_id) REFERENCES events (id) ON DELETE CASCADE,
            UNIQUE(event_id, attendee_email)
        )
    """)
    
    # Add new columns to existing events table if they don't exist
    columns_to_add = [
        ("location_type", "TEXT DEFAULT 'in_person'"),
        ("location_name", "TEXT"),
        ("location_address", "TEXT"),
        ("online_meeting_url", "TEXT"),
        ("max_attendees", "INTEGER"),
        ("is_recurring", "BOOLEAN DEFAULT FALSE"),
        ("recurrence_type", "TEXT"),
        ("recurrence_interval", "INTEGER DEFAULT 1"),
        ("recurrence_end_date", "DATE"),
        ("parent_event_id", "INTEGER")
    ]
    
    for column_name, column_def in columns_to_add:
        try:
            cursor.execute(f"ALTER TABLE events ADD COLUMN {column_name} {column_def}")
        except sqlite3.OperationalError:
            pass  # Column already exists

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

class LocationType(str, Enum):
    IN_PERSON = "in_person"
    ONLINE = "online"
    HYBRID = "hybrid"

class RSVPStatus(str, Enum):
    GOING = "going"
    NOT_GOING = "not_going"
    MAYBE = "maybe"

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
    location_type: LocationType = Field(LocationType.IN_PERSON, description="Type of event location")
    location_name: Optional[str] = Field(None, max_length=200, description="Name of the location")
    location_address: Optional[str] = Field(None, max_length=500, description="Address for in-person events")
    online_meeting_url: Optional[str] = Field(None, max_length=500, description="URL for online meetings")
    max_attendees: Optional[int] = Field(None, ge=1, description="Maximum number of attendees")

    model_config = {
        "json_schema_extra": {
            "example": {
                "title": "Community BBQ",
                "description": "Join us for a fun community barbecue in the park!",
                "event_date": "2024-07-15",
                "event_time": "18:00",
                "organizer": "Community Center",
                "location_type": "in_person",
                "location_name": "Central Park",
                "location_address": "123 Park Avenue, City, State",
                "max_attendees": 50
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
    location_type: Optional[LocationType] = None
    location_name: Optional[str] = Field(None, max_length=200)
    location_address: Optional[str] = Field(None, max_length=500)
    online_meeting_url: Optional[str] = Field(None, max_length=500)
    max_attendees: Optional[int] = Field(None, ge=1)

class RSVPCreate(BaseModel):
    attendee_name: str = Field(..., min_length=1, max_length=100, description="Name of the attendee")
    attendee_email: Optional[str] = Field(None, max_length=255, description="Email of the attendee")
    status: RSVPStatus = Field(RSVPStatus.GOING, description="RSVP status")
    notes: Optional[str] = Field(None, max_length=500, description="Additional notes")

    model_config = {
        "json_schema_extra": {
            "example": {
                "attendee_name": "John Doe",
                "attendee_email": "john@example.com",
                "status": "going",
                "notes": "Looking forward to it!"
            }
        }
    }

class RSVPUpdate(BaseModel):
    status: Optional[RSVPStatus] = None
    notes: Optional[str] = Field(None, max_length=500)

class RSVPResponse(BaseModel):
    id: int
    event_id: int
    attendee_name: str
    attendee_email: Optional[str]
    status: str
    notes: Optional[str]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

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
    location_type: Optional[str]
    location_name: Optional[str]
    location_address: Optional[str]
    online_meeting_url: Optional[str]
    max_attendees: Optional[int]
    rsvp_count: Optional[int] = 0
    going_count: Optional[int] = 0
    maybe_count: Optional[int] = 0
    not_going_count: Optional[int] = 0
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

class EventWithRSVPs(EventResponse):
    rsvps: List[RSVPResponse] = []

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

def get_rsvp_counts(conn, event_id: int) -> dict:
    """Get RSVP counts for an event"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT status, COUNT(*) as count 
        FROM rsvps 
        WHERE event_id = ? 
        GROUP BY status
    """, (event_id,))
    
    counts = {"going": 0, "maybe": 0, "not_going": 0}
    for row in cursor.fetchall():
        counts[row["status"]] = row["count"]
    
    return counts

def safe_get(row, column, default=None):
    """Helper function to safely get column values"""
    try:
        return row[column] if row[column] is not None else default
    except (KeyError, IndexError):
        return default

# API Routes

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    init_db()

@app.get("/", tags=["Root"])
async def root():
    """Welcome message"""
    return {"message": "Community Calendar API with RSVP & Location Support", "version": "2.0.0"}

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
                INSERT INTO events (title, description, date, time, organizer, is_recurring, 
                                  recurrence_type, recurrence_interval, recurrence_end_date,
                                  location_type, location_name, location_address, 
                                  online_meeting_url, max_attendees)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (event.title, event.description, event.event_date, event.event_time, 
                  event.organizer, event.is_recurring, event.recurrence_type, 
                  event.recurrence_interval, event.recurrence_end_date,
                  event.location_type, event.location_name, event.location_address,
                  event.online_meeting_url, event.max_attendees))
            
            event_id = cursor.lastrowid
            conn.commit()
            
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
                    'recurrence_end_date': event.recurrence_end_date,
                    'location_type': event.location_type,
                    'location_name': event.location_name,
                    'location_address': event.location_address,
                    'online_meeting_url': event.online_meeting_url,
                    'max_attendees': event.max_attendees
                }

                recurring_events = generate_recurring_events(base_event_dict)

                for rec_event in recurring_events:
                    cursor.execute("""
                        INSERT INTO events (title, description, date, time, organizer, is_recurring, 
                                          recurrence_type, recurrence_interval, recurrence_end_date, 
                                          parent_event_id, location_type, location_name, location_address,
                                          online_meeting_url, max_attendees)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (rec_event['title'], rec_event['description'], rec_event['event_date'],
                          rec_event['event_time'], rec_event['organizer'], False, None, None, 
                          None, rec_event['parent_event_id'], rec_event['location_type'],
                          rec_event['location_name'], rec_event['location_address'],
                          rec_event['online_meeting_url'], rec_event['max_attendees']))
                    
                conn.commit()

            # Fetch the created event with RSVP counts
            cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            row = cursor.fetchone()
            
            if row:
                rsvp_counts = get_rsvp_counts(conn, event_id)
                
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
                    location_type=safe_get(row, 'location_type', 'in_person'),
                    location_name=safe_get(row, 'location_name'),
                    location_address=safe_get(row, 'location_address'),
                    online_meeting_url=safe_get(row, 'online_meeting_url'),
                    max_attendees=safe_get(row, 'max_attendees'),
                    rsvp_count=sum(rsvp_counts.values()),
                    going_count=rsvp_counts["going"],
                    maybe_count=rsvp_counts["maybe"],
                    not_going_count=rsvp_counts["not_going"],
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
    organizer: Optional[str] = None,
    location_type: Optional[LocationType] = None
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

            if location_type:
                query += " AND location_type = ?"
                params.append(location_type)
            
            query += " ORDER BY date ASC, time ASC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            events = []
            for row in rows:
                rsvp_counts = get_rsvp_counts(conn, row["id"])
                
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
                    location_type=safe_get(row, 'location_type', 'in_person'),
                    location_name=safe_get(row, 'location_name'),
                    location_address=safe_get(row, 'location_address'),
                    online_meeting_url=safe_get(row, 'online_meeting_url'),
                    max_attendees=safe_get(row, 'max_attendees'),
                    rsvp_count=sum(rsvp_counts.values()),
                    going_count=rsvp_counts["going"],
                    maybe_count=rsvp_counts["maybe"],
                    not_going_count=rsvp_counts["not_going"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"])
                ))
            
            return events
    
    except Exception as e:
        print(f"Error fetching events: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch events: {str(e)}")

@app.get("/api/events/{event_id}", response_model=EventWithRSVPs, tags=["Events"])
async def get_event(event_id: int, include_rsvps: bool = False):
    """Get a specific event by ID with optional RSVPs"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            row = cursor.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Event not found")
            
            rsvp_counts = get_rsvp_counts(conn, event_id)
            rsvps = []
            
            if include_rsvps:
                cursor.execute("SELECT * FROM rsvps WHERE event_id = ? ORDER BY created_at", (event_id,))
                rsvp_rows = cursor.fetchall()
                
                for rsvp_row in rsvp_rows:
                    rsvps.append(RSVPResponse(
                        id=rsvp_row["id"],
                        event_id=rsvp_row["event_id"],
                        attendee_name=rsvp_row["attendee_name"],
                        attendee_email=rsvp_row["attendee_email"],
                        status=rsvp_row["status"],
                        notes=rsvp_row["notes"],
                        created_at=datetime.fromisoformat(rsvp_row["created_at"]),
                        updated_at=datetime.fromisoformat(rsvp_row["updated_at"])
                    ))
            
            return EventWithRSVPs(
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
                location_type=safe_get(row, 'location_type', 'in_person'),
                location_name=safe_get(row, 'location_name'),
                location_address=safe_get(row, 'location_address'),
                online_meeting_url=safe_get(row, 'online_meeting_url'),
                max_attendees=safe_get(row, 'max_attendees'),
                rsvp_count=sum(rsvp_counts.values()),
                going_count=rsvp_counts["going"],
                maybe_count=rsvp_counts["maybe"],
                not_going_count=rsvp_counts["not_going"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
                rsvps=rsvps
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
            
            field_mappings = {
                'title': event.title,
                'description': event.description,
                'date': event.event_date,
                'time': event.event_time,
                'organizer': event.organizer,
                'is_recurring': event.is_recurring,
                'recurrence_type': event.recurrence_type,
                'recurrence_interval': event.recurrence_interval,
                'recurrence_end_date': event.recurrence_end_date,
                'location_type': event.location_type,
                'location_name': event.location_name,
                'location_address': event.location_address,
                'online_meeting_url': event.online_meeting_url,
                'max_attendees': event.max_attendees
            }
            
            for db_field, value in field_mappings.items():
                if value is not None:
                    update_fields.append(f"{db_field} = ?")
                    params.append(value)
            
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
            
            rsvp_counts = get_rsvp_counts(conn, event_id)
            
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
                location_type=safe_get(row, 'location_type', 'in_person'),
                location_name=safe_get(row, 'location_name'),
                location_address=safe_get(row, 'location_address'),
                online_meeting_url=safe_get(row, 'online_meeting_url'),
                max_attendees=safe_get(row, 'max_attendees'),
                rsvp_count=sum(rsvp_counts.values()),
                going_count=rsvp_counts["going"],
                maybe_count=rsvp_counts["maybe"],
                not_going_count=rsvp_counts["not_going"],
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
    """Delete an event and all associated RSVPs"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Event not found")
            
            # Delete RSVPs first (due to foreign key constraints)
            cursor.execute("DELETE FROM rsvps WHERE event_id = ?", (event_id,))
            
            # Delete the event and any recurring instances
            cursor.execute("DELETE FROM events WHERE id = ? OR parent_event_id = ?", (event_id, event_id))
            conn.commit()
            
            return {"message": "Event and all RSVPs deleted successfully", "event_id": event_id}
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete event: {str(e)}")

# RSVP Endpoints

@app.post("/api/events/{event_id}/rsvp", response_model=RSVPResponse, tags=["RSVPs"])
async def create_rsvp(event_id: int, rsvp: RSVPCreate):
    """Create an RSVP for an event"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Check if event exists
            cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            event_row = cursor.fetchone()
            if not event_row:
                raise HTTPException(status_code=404, detail="Event not found")
            
            # Check if max attendees limit would be exceeded
            if event_row["max_attendees"]:
                cursor.execute("SELECT COUNT(*) FROM rsvps WHERE event_id = ? AND status = 'going'", (event_id,))
                current_going = cursor.fetchone()[0]
                if rsvp.status == RSVPStatus.GOING and current_going >= event_row["max_attendees"]:
                    raise HTTPException(status_code=400, detail="Event is at maximum capacity")
            
            # Check if RSVP already exists for this email
            if rsvp.attendee_email:
                cursor.execute("SELECT id FROM rsvps WHERE event_id = ? AND attendee_email = ?", 
                             (event_id, rsvp.attendee_email))
                existing = cursor.fetchone()
                if existing:
                    raise HTTPException(status_code=400, detail="RSVP already exists for this email")
            
            cursor.execute("""
                INSERT INTO rsvps (event_id, attendee_name, attendee_email, status, notes)
                VALUES (?, ?, ?, ?, ?)
            """, (event_id, rsvp.attendee_name, rsvp.attendee_email, rsvp.status, rsvp.notes))
            
            rsvp_id = cursor.lastrowid
            conn.commit()
            
            # Fetch the created RSVP
            cursor.execute("SELECT * FROM rsvps WHERE id = ?", (rsvp_id,))
            row = cursor.fetchone()
            
            return RSVPResponse(
                id=row["id"],
                event_id=row["event_id"],
                attendee_name=row["attendee_name"],
                attendee_email=row["attendee_email"],
                status=row["status"],
                notes=row["notes"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"])
            )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating RSVP: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create RSVP: {str(e)}")

@app.get("/api/events/{event_id}/rsvps", response_model=List[RSVPResponse], tags=["RSVPs"])
async def get_event_rsvps(event_id: int, status: Optional[RSVPStatus] = None):
    """Get all RSVPs for an event"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Check if event exists
            cursor.execute("SELECT id FROM events WHERE id = ?", (event_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Event not found")
            
            query = "SELECT * FROM rsvps WHERE event_id = ?"
            params = [event_id]
            
            if status:
                query += " AND status = ?"
                params.append(status)
            
            query += " ORDER BY created_at"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            rsvps = []
            for row in rows:
                rsvps.append(RSVPResponse(
                    id=row["id"],
                    event_id=row["event_id"],
                    attendee_name=row["attendee_name"],
                    attendee_email=row["attendee_email"],
                    status=row["status"],
                    notes=row["notes"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"])
                ))
            
            return rsvps
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching RSVPs: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch RSVPs: {str(e)}")

@app.put("/api/events/{event_id}/rsvp/{rsvp_id}", response_model=RSVPResponse, tags=["RSVPs"])
async def update_rsvp(event_id: int, rsvp_id: int, rsvp_update: RSVPUpdate):
    """Update an RSVP"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Check if RSVP exists and belongs to the event
            cursor.execute("SELECT * FROM rsvps WHERE id = ? AND event_id = ?", (rsvp_id, event_id))
            existing_rsvp = cursor.fetchone()
            if not existing_rsvp:
                raise HTTPException(status_code=404, detail="RSVP not found")
            
            # Check capacity constraints if changing to 'going'
            if rsvp_update.status == RSVPStatus.GOING and existing_rsvp["status"] != "going":
                cursor.execute("SELECT max_attendees FROM events WHERE id = ?", (event_id,))
                event_row = cursor.fetchone()
                if event_row["max_attendees"]:
                    cursor.execute("SELECT COUNT(*) FROM rsvps WHERE event_id = ? AND status = 'going'", (event_id,))
                    current_going = cursor.fetchone()[0]
                    if current_going >= event_row["max_attendees"]:
                        raise HTTPException(status_code=400, detail="Event is at maximum capacity")
            
            # Build update query
            update_fields = []
            params = []
            
            if rsvp_update.status is not None:
                update_fields.append("status = ?")
                params.append(rsvp_update.status)
            
            if rsvp_update.notes is not None:
                update_fields.append("notes = ?")
                params.append(rsvp_update.notes)
            
            if not update_fields:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            params.extend([rsvp_id, event_id])
            
            query = f"UPDATE rsvps SET {', '.join(update_fields)} WHERE id = ? AND event_id = ?"
            cursor.execute(query, params)
            conn.commit()
            
            # Fetch updated RSVP
            cursor.execute("SELECT * FROM rsvps WHERE id = ?", (rsvp_id,))
            row = cursor.fetchone()
            
            return RSVPResponse(
                id=row["id"],
                event_id=row["event_id"],
                attendee_name=row["attendee_name"],
                attendee_email=row["attendee_email"],
                status=row["status"],
                notes=row["notes"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"])
            )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating RSVP: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update RSVP: {str(e)}")

@app.delete("/api/events/{event_id}/rsvp/{rsvp_id}", tags=["RSVPs"])
async def delete_rsvp(event_id: int, rsvp_id: int):
    """Delete an RSVP"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Check if RSVP exists and belongs to the event
            cursor.execute("SELECT * FROM rsvps WHERE id = ? AND event_id = ?", (rsvp_id, event_id))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="RSVP not found")
            
            cursor.execute("DELETE FROM rsvps WHERE id = ? AND event_id = ?", (rsvp_id, event_id))
            conn.commit()
            
            return {"message": "RSVP deleted successfully", "rsvp_id": rsvp_id}
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting RSVP: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete RSVP: {str(e)}")

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
                rsvp_counts = get_rsvp_counts(conn, row["id"])
                
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
                    location_type=safe_get(row, 'location_type', 'in_person'),
                    location_name=safe_get(row, 'location_name'),
                    location_address=safe_get(row, 'location_address'),
                    online_meeting_url=safe_get(row, 'online_meeting_url'),
                    max_attendees=safe_get(row, 'max_attendees'),
                    rsvp_count=sum(rsvp_counts.values()),
                    going_count=rsvp_counts["going"],
                    maybe_count=rsvp_counts["maybe"],
                    not_going_count=rsvp_counts["not_going"],
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
    """Get calendar statistics including RSVP data"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Total events
            cursor.execute("SELECT COUNT(*) FROM events")
            total_events = cursor.fetchone()[0]
            
            # Total RSVPs
            cursor.execute("SELECT COUNT(*) FROM rsvps")
            total_rsvps = cursor.fetchone()[0]
            
            # RSVP breakdown
            cursor.execute("SELECT status, COUNT(*) FROM rsvps GROUP BY status")
            rsvp_breakdown = {row[0]: row[1] for row in cursor.fetchall()}
            
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
            
            # Events by location type
            cursor.execute("SELECT location_type, COUNT(*) FROM events GROUP BY location_type")
            events_by_location = {row[0] or 'in_person': row[1] for row in cursor.fetchall()}
            
            return {
                "total_events": total_events,
                "total_rsvps": total_rsvps,
                "rsvp_breakdown": rsvp_breakdown,
                "events_this_month": events_this_month,
                "upcoming_events": upcoming_events,
                "events_by_location_type": events_by_location,
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
    """Export calendar to ICS format"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM events ORDER BY date ASC, time ASC")
            rows = cursor.fetchall()

            if not rows:
                raise HTTPException(status_code=404, detail="No events found to export")
            
            ics = "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//Community Calendar//EN\n"

            for row in rows:
                event_date = datetime.fromisoformat(row["date"])
                if row["time"]:
                    hours, minutes = map(int, row["time"].split(":"))
                    event_start = event_date.replace(hour=hours, minute=minutes)
                else:
                    event_start = event_date.replace(hour=0, minute=0)

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
                
                # Add location information
                location_type = safe_get(row, 'location_type', 'in_person')
                location_parts = []
                
                if location_type == 'online' and safe_get(row, 'online_meeting_url'):
                    location_parts.append(f"Online: {row['online_meeting_url']}")
                elif location_type in ['in_person', 'hybrid']:
                    if safe_get(row, 'location_name'):
                        location_parts.append(row['location_name'])
                    if safe_get(row, 'location_address'):
                        location_parts.append(row['location_address'])
                    if location_type == 'hybrid' and safe_get(row, 'online_meeting_url'):
                        location_parts.append(f"Online option: {row['online_meeting_url']}")
                
                if location_parts:
                    ics += f"LOCATION:{', '.join(location_parts)}\n"

                # Handle recurrence if enabled
                if safe_get(row, 'is_recurring') and safe_get(row, 'recurrence_type'):
                    freq = row["recurrence_type"].upper()
                    interval = safe_get(row, 'recurrence_interval', 1)
                    rrule = f"FREQ={freq};INTERVAL={interval}"

                    if safe_get(row, 'recurrence_end_date'):
                        until = datetime.fromisoformat(row["recurrence_end_date"]).strftime("%Y%m%dT%H%M%SZ")
                        rrule += f";UNTIL={until}"

                    ics += f"RRULE:{rrule}\n"
                
                ics += "END:VEVENT\n"
            
            ics += "END:VCALENDAR"
            
            return Response(
                content=ics,
                media_type="text/calendar",
                headers={"Content-Disposition": "attachment; filename=community_calendar.ics"}
            )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error exporting as ics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to export calendar: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)