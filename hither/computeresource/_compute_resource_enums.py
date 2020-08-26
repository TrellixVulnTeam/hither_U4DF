# types of messages between job handler and compute resource
# these are communicated via feeds
class MessageTypes:
    # job handler registry
    COMPUTE_RESOURCE_STARTED = 'COMPUTE_RESOURCE_STARTED'
    ADD_JOB_HANDLER = 'ADD_JOB_HANDLER'
    REMOVE_JOB_HANDLER = 'REMOVE_JOB_HANDLER'

    # incoming messages from job handler
    ADD_JOB = 'ADD_JOB'
    CANCEL_JOB = 'CANCEL_JOB'
    KEEP_ALIVE = 'KEEP_ALIVE'

    # outgoing messages to job handler
    JOB_QUEUED = 'JOB_QUEUED'
    JOB_STARTED = 'JOB_STARTED'
    JOB_FINISHED = 'JOB_FINISHED'
    JOB_ERROR = 'JOB_ERROR'
    # KEEP_ALIVE - as above

# keys (or field names) of messages between job handler and compute resource
class MessageKeys:
    TYPE = 'type'
    TIMESTAMP = 'timestamp'
    JOB_ID = 'job_id'
    JOB_SERIALIZED = 'job_serialized'
    LABEL = 'label'
    RUNTIME_INFO = 'runtime_info'
    EXCEPTION = 'exception'
    JOB_HANDLER_URI = 'job_handler_uri'
    RESULT = 'result'
    RESULT_URI = 'result_uri'

class InternalJobAttributeKeys:
    CR_JOB_HASH = '_cr_job_hash'

# A couple of names of subfeeds used for the communication
class SubfeedNames:
    JOB_HANDLER_REGISTRY = 'job_handler_registry'
    MAIN = 'main'