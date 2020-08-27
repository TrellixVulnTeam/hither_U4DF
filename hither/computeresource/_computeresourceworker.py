from typing import List, Dict, Set
import time
import kachery_p2p as kp
import numbers
from .._workerprocess import WorkerProcess
from .._preventkeyboardinterrupt import PreventKeyboardInterrupt
from ._computeresourcejobmanager import ComputeResourceJobManager
from ._jobhandlerconnection import JobHandlerConnection
from ._result_small_enough_to_store_directly import _result_small_enough_to_store_directly
from ._compute_resource_enums import MessageKeys, MessageTypes, SubfeedNames

# The compute resource worker lives in a worker process
class ComputeResourceWorker:
    def __init__(
        self,
        compute_resource_uri, # uri of this compute resource feed
        nodes_with_access # ids of nodes that have privileges of writing to this compute resource
    ):
        self._compute_resource_uri = compute_resource_uri
        self._nodes_with_access = nodes_with_access

        # Load the job handler registry feed and set the access permissions
        feed = kp.load_feed(self._compute_resource_uri)
        subfeed = feed.get_subfeed(SubfeedNames.JOB_HANDLER_REGISTRY)
        subfeed.set_access_rules(dict(
            rules = [
                dict(
                    nodeId=n['node_id'],
                    write=True
                )
                for n in self._nodes_with_access
            ]
        ))
        self._subfeed = subfeed
        # move to the end of the registry subfeed
        self._subfeed.set_position(self._subfeed.get_num_messages())
        self._subfeed.append_message({
            MessageKeys.TYPE: MessageTypes.COMPUTE_RESOURCE_STARTED,
            MessageKeys.TIMESTAMP: time.time() - 0
        })
    def handle_message_from_parent(self, message):
        pass
    def iterate(self):
        # listen for messages on the job handler registry subfeed
        try:
            messages = self._subfeed.get_next_messages(wait_msec=3000)
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