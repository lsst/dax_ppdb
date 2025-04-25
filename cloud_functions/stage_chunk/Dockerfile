FROM gcr.io/dataflow-templates-base/python3-template-launcher-base:latest

# Copy your code inside
WORKDIR /dataflow/template
COPY stage_chunk_beam_job.py .

# Install dependencies (Beam is already installed in base image)
RUN pip install --no-cache-dir apache-beam[gcp]==2.54.0

# Fix dependency mismatch manually
RUN pip install --no-cache-dir --upgrade google-auth-httplib2

# Set the entry point
ENTRYPOINT ["python", "stage_chunk_beam_job.py"]