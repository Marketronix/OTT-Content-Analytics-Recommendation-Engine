# scripts/setup/create_pubsub_topics.py
import argparse
import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_topic(project_id, topic_id):
    """Create a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    
    try:
        topic = publisher.create_topic(request={"name": topic_path})
        print(f"Created topic: {topic.name}")
        return topic
    except Exception as e:
        print(f"Topic {topic_path} already exists: {e}")
        return publisher.get_topic(request={"topic": topic_path})

def create_subscription(project_id, topic_id, subscription_id, enable_exactly_once=False):
    """Create a Pub/Sub subscription."""
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    # Configure the subscription
    push_config = None  # Use pull delivery
    ack_deadline_seconds = 60  # 60 seconds
    message_retention_duration = {"seconds": 86400}  # 1 day
    
    # Create subscription options
    request = {
        "name": subscription_path,
        "topic": topic_path,
        "push_config": push_config,
        "ack_deadline_seconds": ack_deadline_seconds,
        "message_retention_duration": message_retention_duration,
        "enable_exactly_once_delivery": enable_exactly_once,
    }
    
    try:
        subscription = subscriber.create_subscription(request=request)
        print(f"Created subscription: {subscription.name}")
        return subscription
    except Exception as e:
        print(f"Subscription {subscription_path} already exists: {e}")
        return subscriber.get_subscription(request={"subscription": subscription_path})

def setup_pubsub_infrastructure(project_id):
    """Set up the Pub/Sub infrastructure for streaming events."""
    # Create the raw events topic
    raw_events_topic = "ott-raw-events"
    create_topic(project_id, raw_events_topic)
    
    # Create subscription for the raw events topic
    raw_events_subscription = "ott-raw-events-sub"
    create_subscription(project_id, raw_events_topic, raw_events_subscription, enable_exactly_once=True)
    
    # Create the processed events topic
    processed_events_topic = "ott-processed-events"
    create_topic(project_id, processed_events_topic)
    
    # Create subscription for the processed events topic
    processed_events_subscription = "ott-processed-events-sub"
    create_subscription(project_id, processed_events_topic, processed_events_subscription, enable_exactly_once=True)
    
    print("Pub/Sub infrastructure setup complete.")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Set up Pub/Sub topics and subscriptions")
    parser.add_argument("--project_id", required=False, default=os.getenv('GCP_PROJECT_ID'),
                        help="Google Cloud Project ID")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    setup_pubsub_infrastructure(args.project_id)