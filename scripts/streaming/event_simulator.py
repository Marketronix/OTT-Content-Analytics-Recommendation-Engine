# dags/scripts/event_simulator.py
import argparse
import datetime
import json
import random
import time
import uuid
from google.cloud import pubsub_v1

def generate_event(user_id, content_id, event_type=None):
    """Generate a single event."""
    if not event_type:
        event_type = random.choice(["start", "pause", "resume", "complete", "seek", "rate"])
        
    event = {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "user_id": user_id,
        "content_id": content_id,
        "event_type": event_type,
        "timestamp": datetime.datetime.now().isoformat(),
        "session_id": f"session_{uuid.uuid4().hex[:10]}",
        "position": random.randint(0, 7200) if event_type != "start" else 0,
        "device": {
            "type": random.choice(["mobile", "tv", "web", "tablet"]),
            "os": random.choice(["iOS", "Android", "Windows", "macOS", "tvOS"]),
            "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge", "app"]),
        },
        "location": {
            "country": random.choice(["US", "GB", "CA", "AU", "DE", "FR", "IN"]),
            "region": f"Region_{random.randint(1, 50)}",
            "city": f"City_{random.randint(1, 100)}"
        },
        "quality": {
            "resolution": random.choice(["SD", "HD", "4K"]),
            "bitrate": random.randint(800, 15000)
        }
    }
    
    if event_type == "rate":
        event["rating"] = random.randint(1, 5)
        
    return event

def publish_events(project_id, topic_name, num_users, events_per_user, rate_limit):
    """Generate and publish events to Pub/Sub."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    # Generate some user IDs
    users = [f"user_{uuid.uuid4().hex[:8]}" for _ in range(num_users)]
    
    # Generate some content IDs (using IMDb format)
    content_ids = [f"tt{random.randint(1, 9999999):07d}" for _ in range(100)]
    
    total_events = 0
    start_time = time.time()
    
    for user_id in users:
        for _ in range(events_per_user):
            # Choose a random content ID
            content_id = random.choice(content_ids)
            
            # Generate an event
            event = generate_event(user_id, content_id)
            
            # Convert to JSON and publish
            data = json.dumps(event).encode("utf-8")
            publisher.publish(topic_path, data=data)
            
            total_events += 1
            
            # Sleep to respect rate limit
            if rate_limit > 0:
                time.sleep(1.0 / rate_limit)
    
    elapsed_time = time.time() - start_time
    print(f"Published {total_events} events in {elapsed_time:.2f} seconds")
    print(f"Average rate: {total_events / elapsed_time:.2f} events/second")

def main():
    parser = argparse.ArgumentParser(description="Generate and publish OTT events")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--topic_name", required=True, help="Pub/Sub topic name")
    parser.add_argument("--user_count", type=int, default=10, help="Number of users")
    parser.add_argument("--events_per_user", type=int, default=5, help="Events per user")
    parser.add_argument("--rate_limit", type=float, default=10.0, help="Events per second limit")
    
    args = parser.parse_args()
    publish_events(args.project_id, args.topic_name, args.user_count, args.events_per_user, args.rate_limit)

if __name__ == "__main__":
    main()