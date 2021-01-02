import pathlib
import sys

directory = pathlib.Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(directory))
