import time
import os
import threading
import _thread

class _Thread(threading.Thread):
    def __init__(self, directory: str):
        super(_Thread, self).__init__()
        self._stop = threading.Event()
        self._directory = directory
  
    def stop(self):
        self._stop.set()
  
    def stopped(self):
        return self._stop.is_set()
  
    def run(self):
        while True:
            if self.stopped():
                return
            if not os.path.exists(self._directory):
                print(f'Ending process because directory no longer exists: {self._directory}')
                _thread.interrupt_main()
                return
            time.sleep(2)

class EndProcessWhenDirectoryDisappears(object):
    def __init__(self, directory: str):
        self._directory = directory
    def __enter__(self):
        self._thread = _Thread(directory=self._directory)
        self._thread.start()
    def __exit__(self, type, value, traceback):
        self._thread.stop()

