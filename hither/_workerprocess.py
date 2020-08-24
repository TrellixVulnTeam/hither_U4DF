import time
import multiprocessing

class WorkerProcess:
    def __init__(self, WorkerClass, args):
        self._WorkerClass = WorkerClass
        pipe_to_parent, pipe_to_child = multiprocessing.Pipe()
        self._pipe_to_process = pipe_to_child
        self._process = multiprocessing.Process(
            target=_worker_process_target,
            args=(pipe_to_parent, WorkerClass, args)
        )
        self._on_message_from_process_callbacks = []
        self._is_running = False
    def start(self):
        self._process.start()
        self._is_running = True
    def stop(self):
        self._pipe_to_process.send({'type': 'exit'})
        self._is_running = False
    def is_running(self):
        return self._is_running
    def send_message_to_process(self, message):
        self._pipe_to_process.send({'type': 'message', 'message': message})
    def on_message_from_process(self, cb):
        self._on_message_from_process_callbacks.append(cb)
    def iterate(self):
        if not self._is_running:
            return
        while self._pipe_to_process.poll():
            x = self._pipe_to_process.recv()
            if x['type'] == 'exit':
                self.stop()
            elif x['type'] == 'message':
                for cb in self._on_message_from_process_callbacks:
                    cb(x['message'])

def _worker_process_target(pipe_to_parent, WorkerClass, args):
    if not isinstance(args, tuple):
        args = (args, )
    worker = WorkerClass(*args)
    def send_message_to_parent(message):
        pipe_to_parent.send({'type': 'message', 'message': message})
    def handle_exit():
        pipe_to_parent.send({'type': 'exit'})
    setattr(worker, 'send_message_to_parent', send_message_to_parent)
    setattr(worker, 'exit', handle_exit)
    while True:
        while pipe_to_parent.poll():
            x = pipe_to_parent.recv()
            if x['type'] == 'exit':
                return
            elif x['type'] == 'message':
                worker.handle_message_from_parent(x['message'])
        worker.iterate()
        time.sleep(0.1)

class ExampleWorkerClass:
    def __init__(self, arg1, arg2):
        self._arg1 = arg1
        self._arg2 = arg2
    def handle_message_from_parent(self, message):
        # do something with message from parent
        pass
    def iterate(self):
        # do something and perhaps call self.send_message_to_parent(...) or self.exit()
        pass
    # The following methods will be overwritten by the framework
    # They are just placeholders to keep linters happy
    def send_message_to_parent(self, message): # overwritten by framework
        pass
    def exit(self): # overwritten by framework
        pass
