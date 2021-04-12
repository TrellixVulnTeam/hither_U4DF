import time
import os
import threading
import _thread
from typing import Union

class _Thread(threading.Thread):
    def __init__(self, path: str):
        super(_Thread, self).__init__()
        self._stop = threading.Event()
        self._path = path
  
    def stop(self):
        self._stop.set()
  
    def stopped(self):
        return self._stop.is_set()
  
    def run(self):
        while True:
            if self.stopped():
                return
            if not os.path.exists(self._path):
                print(f'Ending process because file no longer exists: {self._path}')
                _thread.interrupt_main()
                return
            time.sleep(2)

class EndProcessWhenFileDisappears(object):
    def __init__(self, path: Union[str, None]):
        self._path = path
    def __enter__(self):
        if self._path is not None:
            self._thread = _Thread(path=self._path)
            self._thread.start()
    def __exit__(self, type, value, traceback):
        if self._path is not None:
            self._thread.stop()

