def ingest(event, context):
    print(f"File finalized: {event['name']} in bucket {event['bucket']}")
