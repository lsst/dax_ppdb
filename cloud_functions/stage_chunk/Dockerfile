FROM gcr.io/dataflow-templates-base/python3-template-launcher-base:latest

# Copy your pipeline code inside the container
WORKDIR /dataflow/template
COPY stage_chunk_beam_job.py .

# Install additional dependencies (optional)
RUN pip install --no-cache-dir apache-beam[gcp]==2.54.0
RUN pip install --no-cache-dir --upgrade google-auth-httplib2

# Set required environment variable for Flex Templates
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=stage_chunk_beam_job.py

# Use the standard launcher ENTRYPOINT
ENTRYPOINT ["/opt/apache/beam/boot"]