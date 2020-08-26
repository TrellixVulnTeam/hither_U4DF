import kachery_p2p as kp
from ._compute_resource_enums import SubfeedNames

# The worker associated with the job handler connection
# It listens for incoming messages from the job handler
class JobHandlerConnectionWorker:
    def __init__(self, job_handler_uri):
        self._job_handler_uri = job_handler_uri
        # Load the subfeed of incoming messages from the job handler
        num_tries = 6
        self._incoming_subfeed = None
        for _ in range(num_tries):
            try:
                self._incoming_subfeed = kp.load_feed(job_handler_uri, timeout_sec=2).get_subfeed(SubfeedNames.MAIN)
                break
            except:
                pass
        if self._incoming_subfeed is None:
            raise Exception(f'Problem loading incoming feed for job handler: {job_handler_uri}')
    def handle_message_from_parent(self, message):
        pass
    def iterate(self):
        # Listen for messages from the job handler and send to parent
        try:
            messages = self._incoming_subfeed.get_next_messages(wait_msec=3000)
        except:
            # perhaps the daemon is down
            messages = None
        if messages is not None:
            for message in messages:
                self.send_message_to_parent(message)
    # The following methods will be overwritten by the framework
    # They are just placeholders to keep linters happy
    def send_message_to_parent(self, message): # overwritten by framework
        pass
    def exit(self): # overwritten by framework
        pass