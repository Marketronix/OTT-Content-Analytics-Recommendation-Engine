# scripts/streaming/event_simulator.py
import argparse
import datetime
import json
import random
import time
import uuid
from typing import Dict, List
import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PROJECT_ID = os.getenv('GCP_PROJECT_ID')

# Event types with weights
EVENT_TYPES = {
    "start": 30,
    "pause": 20,
    "resume": 20,
    "complete": 10,
    "seek": 15,
    "rate": 5
}

# Device types with weights
DEVICE_TYPES = {
    "mobile": 40,
    "tv": 30,
    "web": 20,
    "tablet": 10
}

# OS types with weights
OS_TYPES = {
    "iOS": 30,
    "Android": 30,
    "Windows": 15,
    "macOS": 15,
    "tvOS": 5,
    "Linux": 5
}

# Browser types with weights
BROWSER_TYPES = {
    "Chrome": 40,
    "Safari": 25,
    "Firefox": 10,
    "Edge": 5,
    "app": 20
}

# Streaming quality options with weights
RESOLUTIONS = {
    "SD": 30,
    "HD": 50,
    "4K": 20
}

# Bitrates by resolution (kbps)
BITRATES = {
    "SD": [800, 1200, 1500],
    "HD": [2500, 3500, 5000],
    "4K": [8000, 12000, 15000]
}

# Countries with weights
COUNTRIES = {
    "US": 50,
    "GB": 10,
    "CA": 8,
    "AU": 7,
    "DE": 5,
    "FR": 5,
    "IN": 5,
    "JP": 3,
    "BR": 3,
    "MX": 2,
    "Other": 2
}

# Sample session durations in minutes
SESSION_DURATIONS = [15, 30, 45, 60, 90, 120, 180]

def get_weighted_choice(options: Dict[str, int]) -> str:
    """Get a random choice based on weights."""
    total = sum(options.values())
    r = random.uniform(0, total)
    cumulative = 0
    for item, weight in options.items():
        cumulative += weight
        if r <= cumulative:
            return item
    return list(options.keys())[0]

def generate_users(count: int) -> List[str]:
    """Generate a list of user IDs."""
    return [f"user_{uuid.uuid4().hex[:8]}" for _ in range(count)]

def get_content_ids(count: int) -> List[str]:
    """Generate sample content IDs."""
    return [f"tt{random.randint(1, 9999999):07d}" for _ in range(count)]

def generate_event(user_id: str, content_ids: List[str], session_id: str = None,
                  previous_position: int = None, content_duration: int = None) -> Dict:
    """Generate a single event."""
    # Select a random content ID
    content_id = random.choice(content_ids)

    # If content duration is not provided, generate one (in seconds)
    if not content_duration:
        content_duration = random.randint(20 * 60, 180 * 60)

    # Get event type
    event_type = get_weighted_choice(EVENT_TYPES)

    # Generate or reuse session ID
    if not session_id:
        session_id = f"session_{uuid.uuid4().hex[:10]}"

    # Position in content
    if previous_position is None:
        if event_type == "start":
            position = 0
        else:
            position = random.randint(0, content_duration)
    else:
        if event_type == "resume":
            position = previous_position
        elif event_type == "seek":
            position = random.randint(0, content_duration)
        elif event_type == "complete":
            position = content_duration
        else:
            # For other events, increment the position
            position = min(previous_position + random.randint(1, 300), content_duration)

    # Duration for the event (for seeking or playing)
    if event_type in ["start", "resume", "seek"]:
        remaining_time = content_duration - position
        if remaining_time > 0:
            duration = random.randint(1, min(300, remaining_time))
        else:
            duration = 0
    else:
        duration = None

    # Device information
    device_type = get_weighted_choice(DEVICE_TYPES)
    os_type = get_weighted_choice(OS_TYPES)
    browser = get_weighted_choice(BROWSER_TYPES) if device_type != "tv" else "app"

    # Location information
    country = get_weighted_choice(COUNTRIES)
    if country == "Other":
        country = random.choice(["ES", "IT", "NL", "SE", "KR", "SG", "ZA"])

    # Streaming quality
    resolution = get_weighted_choice(RESOLUTIONS)
    bitrate = random.choice(BITRATES[resolution])

    # Rating (only for rate events)
    rating = random.randint(1, 5) if event_type == "rate" else None

    # Create the event
    event = {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "user_id": user_id,
        "content_id": content_id,
        "event_type": event_type,
        "timestamp": datetime.datetime.now().isoformat(),
        "session_id": session_id,
        "duration": duration,
        "position": position,
        "device": {
            "type": device_type,
            "os": os_type,
            "browser": browser,
            "model": f"{device_type.capitalize()} {random.choice(['X', 'Pro', 'Max', 'Ultra', 'S'])}" if device_type in ["mobile", "tablet"] else None
        },
        "location": {
            "country": country,
            "region": f"Region_{random.randint(1, 50)}",
            "city": f"City_{random.randint(1, 100)}"
        },
        "quality": {
            "resolution": resolution,
            "bitrate": bitrate
        }
    }

    if rating:
        event["rating"] = rating

    return event, session_id, position, content_duration

def publish_to_pubsub(project_id: str, topic_name: str, events: List[Dict]):
    """Publish events to Pub/Sub."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    futures = []
    for event in events:
        data = json.dumps(event).encode("utf-8")
        future = publisher.publish(topic_path, data=data)
        futures.append(future)

    # Wait for all publish futures to complete
    for future in futures:
        future.result()

    print(f"Published {len(events)} events to {topic_path}")

def run_simulation(project_id: str, topic_name: str, user_count: int, events_per_user: int,
                  rate_limit: float, content_count: int = 1000):
    """Run the event simulation."""
    # Generate users and content IDs
    users = generate_users(user_count)
    content_ids = get_content_ids(content_count)

    # Track sessions for users
    user_sessions = {}

    total_events = 0

    # Generate events_per_user events for each user
    for user_id in users:
        session_id = None
        position = None
        content_duration = None
        content_id = None

        for i in range(events_per_user):
            # Generate an event
            event, session_id, position, content_duration = generate_event(
                user_id, content_ids, session_id, position, content_duration
            )

            # Publish to Pub/Sub
            publish_to_pubsub(project_id, topic_name, [event])
            total_events += 1

            # Sleep to respect rate limit
            time.sleep(1.0 / rate_limit)

            # Sometimes create a new session
            if random.random() < 0.3 and i < events_per_user - 1:
                session_id = None
                position = None
                content_duration = None

    print(f"Simulation complete. Generated {total_events} events for {user_count} users.")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Simulate OTT streaming events")
    parser.add_argument("--project_id", type=str, required=False,
                        default=os.getenv('GCP_PROJECT_ID'),
                        help="Google Cloud Project ID")
    parser.add_argument("--topic_name", type=str, required=True,
                        help="Pub/Sub topic name")
    parser.add_argument("--user_count", type=int, default=100,
                        help="Number of users to simulate")
    parser.add_argument("--events_per_user", type=int, default=10,
                        help="Number of events to generate per user")
    parser.add_argument("--rate_limit", type=float, default=10.0,
                        help="Maximum events per second")
    parser.add_argument("--content_count", type=int, default=1000,
                        help="Number of content items to use")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    run_simulation(
        args.project_id,
        args.topic_name,
        args.user_count,
        args.events_per_user,
        args.rate_limit,
        args.content_count
    )
