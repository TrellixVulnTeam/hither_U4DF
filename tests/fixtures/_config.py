MONGO_PORT = 27027
COMPUTE_RESOURCE_ID = 'test_compute_resource_001'
DATABASE_NAME = 'test_database_001'
KACHERY_PORT = 3602
EVENT_STREAM_SERVER_PORT=29002

KACHERY_CONFIG = dict(
    url=f'http://localhost:{KACHERY_PORT}',
    channel="test-channel",
    password="test-password"
)

EVENT_STREAM_SERVER_CONFIG = dict(
    url=f'http://localhost:{EVENT_STREAM_SERVER_PORT}',
    channel="readwrite",
    password="readwrite"
)
