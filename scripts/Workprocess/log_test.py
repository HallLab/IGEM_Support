import logging

# create a logger
logger = logging.getLogger(__name__)

# configure the logger
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# log a message
v_desc = " pen file without skips rows and rows per block process."
logger.info(v_desc)
