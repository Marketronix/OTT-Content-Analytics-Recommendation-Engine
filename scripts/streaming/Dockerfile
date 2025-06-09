# scripts/streaming/Dockerfile
FROM apache/beam_python3.9_sdk:2.42.0

WORKDIR /app

# Copy the pipeline code
COPY event_pipeline.py ./

# Install dependencies
RUN pip install --no-cache-dir google-cloud-pubsub==2.13.6 \
                             google-cloud-bigquery==3.3.5

# Set the entrypoint
ENTRYPOINT ["python", "/app/event_pipeline.py"]