import os
import sys
from pathlib import Path

v_root = Path(__file__).parents[2]
sys.path.append(os.path.abspath(v_root))

# from igem import server  # noqa E402
from igem import ge  # noqa E402

"funcoes de ETL no Server"
# v_return = server.etl.collect(connector='ctdgpassoc')
# v_return = server.etl.prepare(connector='ctdgpassoc', chunk=100)
# v_return = server.etl.map(connector='ctdgpassoc')
# v_return = server.etl.reduce(connector='ctdgpassoc')



"Funcao para sincrinicar a base Cliente"
# v_return = ge.db.sync_db(table='connector')
# v_return = ge.db.sync_db(table='datasource', source='/users/andrerico/dev')