import os

BASE_PATH = os.path.abspath(os.path.dirname(__file__))
SERVER_MODE = "server"
CLIENT_MODE = "client"

################################################################
#          API TOKENS
################################################################
FXCM_TOKEN = "ade85c332715e0892bc9834a78804f019b946b2c"
FXCM_USER = "D261229642"
FXCM_PASS = "Wioh6"
FX_MARKET_API_TOKEN = "gY3ENhVy21qzuDhqwe48"
OANDA_TOKEN = "fc8c1e9ce75dad183e0f96379f71e1ca-ca3279bdd73c2dbc7e7add99fae57ad"
FIXERIO_TOKEN = "aa3c6ee15f441352f8a9b85a468da8b0"
WORLD_TRADING_DATA_TOKEN = "BcxmiBNTN9OTAdLDAZzMRQfpn1nl2SQcEbLJbzCpRRd6hUCa5tyChRVGkDQ6"

################################################################
#          FILE SYSTEM
################################################################
CONFIG_DIR = os.path.dirname(os.path.realpath(__file__))
CODEBASE_DIR = os.path.dirname(os.path.realpath(__file__))
DATASET_DIR = os.path.dirname(os.path.realpath(__file__))
CRAWLER_DIR = ""

################################################################
#           DEFAULT LOGGING
###############################################################
DEFAULT_LOG_LEVEL = "info"
DEFAULT_LOG_CONFIG = os.path.join(BASE_PATH, "adit.log.ini")

