import os

################################################################
#          API TOKENS
################################################################
FXCM_TOKEN = "ade85c332715e0892bc9834a78804f019b946b2c" #https://apiwiki.fxcorporate.com/api/RestAPI/Socket%20REST%20API%20Specs.pdf?_gl=1*1lguir3*_gcl_aw*R0NMLjE1OTEyMTkxNTQuQ2owS0NRandsTjMyQlJDQ0FSSXNBRFotSjR0djgwWV9OYm5heE1XLTZUV3JkMVZvRk93TUVZUm5JbDREbzZ5QXdiRVE5dE5mQnpaSkxfZ2FBbkFkRUFMd193Y0I.*_gcl_dc*R0NMLjE1OTEyMTkxNTQuQ2owS0NRandsTjMyQlJDQ0FSSXNBRFotSjR0djgwWV9OYm5heE1XLTZUV3JkMVZvRk93TUVZUm5JbDREbzZ5QXdiRVE5dE5mQnpaSkxfZ2FBbkFkRUFMd193Y0I.&_ga=2.225277650.995319551.1591213709-378809649.1591098111&_gac=1.254088570.1591219167.Cj0KCQjwlN32BRCCARIsADZ-J4tv80Y_NbnaxMW-6TWrd1VoFOwMEYRnIl4Do6yAwbEQ9tNfBzZJL_gaAnAdEALw_wcB
FXCM_USER = "D261229642"
FXCM_PASS = "Wioh6"
FX_MARKET_API_TOKEN = "gY3ENhVy21qzuDhqwe48"
OANDA_TOKEN = "fc8c1e9ce75dad183e0f96379f71e1ca-ca3279bdd73c2dbc7e7add99fae57ad" #http://developer.oanda.com/rest-live-v20/introduction/
FIXERIO_TOKEN = "aa3c6ee15f441352f8a9b85a468da8b0" #https://fixer.io/quickstart
WORLD_TRADING_DATA_TOKEN = "BcxmiBNTN9OTAdLDAZzMRQfpn1nl2SQcEbLJbzCpRRd6hUCa5tyChRVGkDQ6"

################################################################
#          FILE SYSTEM
################################################################
CONFIG_DIR = os.path.dirname(os.path.realpath(__file__))
CODEBASE_DIR = os.path.dirname(os.path.realpath(__file__))
DATASET_DIR = os.path.dirname(os.path.realpath(__file__))
CRAWLER_DIR = ""
DRIVER_DIR = ""

print(CODEBASE_DIR)
