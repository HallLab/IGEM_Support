import os
import sys
from pathlib import Path

try:
    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))

except Exception as e:
    print("erro: ", e)
    raise

from igem import server  # noqa E402

result = server.sql.load_data(
    table="wordterm",
    path=("/users/andrerico/desktop/gene.csv")
    )

print(result)
