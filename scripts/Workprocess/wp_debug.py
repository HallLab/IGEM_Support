import os
import sys
from pathlib import Path

v_root = Path(__file__).parents[2]
sys.path.append(os.path.abspath(v_root))

from igem import ge  # noqa E402

v_return = ge.etl.collect(connector='hmdburine')

v_return = ge.etl.prepare(connector='hmdburine', chunk=100)

v_return = ge.etl.map(connector='hmdburine')
v_return = ge.etl.reduce(connector='hmdburine')
