# Clase utilizada para las funciones en elpers_2.py / se descarta para evitar complejidad de dependencias, rendimiento y xq se usará Spark =)


#Clase extraída de repositorio / Workspace/Repos/rodrigoasencios@bcp.com.pe
from functools import wraps
import io
import sys

# suppress prints
class PrintSuppressor:
    def __call__(self, func = None, *args, **kwargs):
        # If func is None, this means it's being called directly
        if func is None:
            return self.suppress_prints
        else:
            # otherwise it is used the decorator
            def wrapper(*args, **kwargs):
                return self.suppress_prints(func, *args, **kwargs)
            return wrapper

    def suppress_prints(self, func, *args, **kwargs):
        # backup original stdout
        original_stdout = sys.stdout

        # redirect stdout to null
        sys.stdout = io.StringIO()

        try:
            # Call the function and return its result
            return func(*args, **kwargs)
        finally:
            sys.stdout = original_stdout