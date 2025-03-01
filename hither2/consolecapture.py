from typing import Any, Dict, List, Union
import sys
import time
import io

class CustomStdout():
    def __init__(self, label: str, lines: List[dict], original_stdout, stderr: bool=False, show_console: bool=True):
        self._label = label
        self._lines = lines
        self._original_stdout = original_stdout
        self._stderr = False
        self._show_console = show_console

    def write(self, data: str) -> None:
        lines = data.splitlines(keepends=False)
        for line in lines:
            if line.strip():
                a: Dict[str, Union[float, str, bool]] = dict(
                    timestamp=time.time() - 0,
                    text=line,
                    stderr=self._stderr
                )
                self._lines.append(a)
                if self._show_console:
                    print('{} {}: {}'.format(self._label, _fmt_time(a['timestamp']), a['text']), file=self._original_stdout)
        
    def flush(self) -> None:
        pass

def _fmt_time(t):
    import datetime
    return datetime.datetime.fromtimestamp(t).isoformat()

class ConsoleCapture():
    def __init__(self, label: str='', show_console: bool=True):
        self._label = label
        self._lines: List[dict] = []
        self._time_start = None
        self._time_stop = None
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        self._show_console = show_console

    def __enter__(self):
        self._start_capturing()
        return self

    def __exit__(self, type, value, traceback):
        self._stop_capturing()

    def _start_capturing(self) -> None:
        self._time_start = time.time()
        sys.stdout = CustomStdout(self._label, self._lines, self._original_stdout, show_console=self._show_console)
        sys.stderr = CustomStdout(self._label, self._lines, self._original_stderr, stderr=True, show_console=self._show_console)

    def _stop_capturing(self) -> None:
        self._time_stop = time.time()
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout = self._original_stdout
        sys.stderr = self._original_stderr

    @property
    def label(self):
        return self._label

    @property
    def lines(self) -> List[dict]:
        return self._lines