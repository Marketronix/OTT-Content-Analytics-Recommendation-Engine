# scripts/streaming/Dockerfile
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Copy the pipeline code
COPY event_pipeline.py .

# Install dependencies
RUN pip install --no-cache-dir google-cloud-pubsub==2.13.6 \
                             google-cloud-bigquery==3.3.5 \
                             apache-beam[gcp]==2.42.0

# Metadata for the template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/event_pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"

# Create a requirements file
RUN echo "apache-beam[gcp]==2.42.0" > requirements.txt && \
    echo "google-cloud-pubsub==2.13.6" >> requirements.txt && \
    echo "google-cloud-bigquery==3.3.5" >> requirements.txt