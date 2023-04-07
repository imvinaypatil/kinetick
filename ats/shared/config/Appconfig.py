import os


class Appconfig:
    BLOTTER_ZMQ_HOST = os.environ.get("BLOTTER_ZMQ_HOST")
    BLOTTER_ZMQ_PORT = os.environ.get("BLOTTER_ZMQ_PORT")
    BLOTTER_ZMQ_TOPIC = "_kinetick_"
    LOGLEVEL = "INFO"
