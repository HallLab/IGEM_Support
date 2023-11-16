import time

from django.conf import settings

from igem import ge  # noqa E402


def backup_routine():
    v_return = ge.db.backup(table='all', path_out='v_path')
    return v_return


def clean_routine():
    v_return = ge.db.truncate_table(table='termmap')
    v_return = ge.db.truncate_table(table='term')
    v_return = ge.db.truncate_table(table='termcategory')
    v_return = ge.db.truncate_table(table='termgroup')
    v_return = ge.db.truncate_table(table='wfcontrol')
    v_return = ge.db.truncate_table(table='connector')
    v_return = ge.db.truncate_table(table='datasource')
    v_return = ge.db.truncate_table(table='snpgene')
    return v_return


def restore_routine():
    v_return = ge.db.restore(table='datasource', path_out='v_path')
    time.sleep(5)
    v_return = ge.db.restore(table='connector', path_out='v_path')
    time.sleep(5)
    v_return = ge.db.restore(table='termgroup', path_out='v_path')
    time.sleep(5)
    v_return = ge.db.restore(table='termcategory', path_out='v_path')
    time.sleep(5)
    v_return = ge.db.restore(table='term', path_out='v_path')
    time.sleep(5)
    v_return = ge.db.restore(table='wfcontrol', path_out='v_path')
    time.sleep(5)
    v_return = ge.db.restore(table='termmap', path_out='v_path')
    time.sleep(5)
    v_return = ge.db.restore(table='snpgene', path_out='v_path')
    time.sleep(5)
    return v_return


if __name__ == "__main__":
    # Global Variables
    log_file = __name__
    v_path = str(settings.BASE_DIR) + "/psa/igem/'"
    v_time_process = time.time()
    backup_routine()
