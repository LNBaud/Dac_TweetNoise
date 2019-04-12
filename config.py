import os
# OAuth authentification keys to Twitter API
# in format :
# (
# consumer_key,
# consumer_secret,
# oauth_token,
# oauth_token_secret
# )
ACCESS = (
        "yW71sQxEgKJft22IpEbCASIva",
        "8FXIjck1gJCHmEyCQpUXIza0BvEnVV4zLCKywArtUDwzOu4xPn",
        "1050039997083111424-F19vZDpIWz7un9BiyKOsugCWwY4fvh",
        "aiXu1Sqtv3bEBB4roMWPVf0kPw9Nql9EMxKEX76IjijN3"
    )

# Restricts tweets to the given language, given by an ISO 639-1 code.
# Language detection is best-effort.
# To fetch all tweets, choose LANG = None
LANG = 'fr'

# Directory where tweets files are stored
FILEDIR = "C:\\Users\\Public\\Documents\\"

# Number of tweets in each file
FILEBREAK = 5000

PROXY = {'http': '',
         'https': ''}

# MongoDB cluster config
MONGODB = {
    "USER": "admin",
    "PASSWORD": "Twitt3rN0ise",
    "HOST": "twitternoise-3imjq.mongodb.net",
    "PORT": 27017,
    "DATABASE": "db_fr2"
}