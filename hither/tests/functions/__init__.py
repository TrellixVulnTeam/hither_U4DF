from types import SimpleNamespace

from .zeros import zeros
from .ones import ones
from .add import add
from .mult import mult
from .write_text_file import write_text_file
from .intentional_error import intentional_error
from .do_nothing import do_nothing
from .bad_container import bad_container
from .additional_file import additional_file
from .local_module import local_module
from .identity import identity2
from .arraysum import arraysum

functions = SimpleNamespace(
    zeros=zeros,
    ones=ones,
    add=add,
    mult=mult,
    write_text_file=write_text_file,
    intentional_error=intentional_error,
    do_nothing=do_nothing,
    bad_container=bad_container,
    additional_file=additional_file,
    local_module=local_module,
    identity=identity2,
    arraysum=arraysum
)

