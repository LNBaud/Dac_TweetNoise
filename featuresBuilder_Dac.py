# -*- coding: utf-8 -*-
"""
Created on Mon Oct 29 09:35 2018

@author: dshi, hbaud, vlefranc
"""

import logging
from datetime import datetime, timezone
import csv
from Emojilist import emojilist
from pymongo import MongoClient
import enchant
import time
import sys
sys.path.append('..')
from config import FILEDIR, FILEBREAK, MONGODB

import fr_core_news_md
nlp = fr_core_news_md.load()

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)


class FeaturesBuilder:
    """
    Retrieve data from the MongoDB database.

    """
    def __init__(self):
        print('initialisation...')

        self.do_continue = True
        self.count = 0
        self.line_count = 0
        self.current_file = FILEDIR + "data_dac.csv"
        # connect to MongoDB
        client = MongoClient("mongodb+srv://" + MONGODB["USER"] + ":" + MONGODB["PASSWORD"] + "@" + MONGODB["HOST"] + "/" + MONGODB["DATABASE"] + "?retryWrites=true")
        self.db = client[MONGODB["DATABASE"]]

    def retrieve(self):
        start = time.time()
        logging.info("Retrieving data...")
        tweets = self.db.tweets.find({"spam": {"$exists": True}})
        logging.info("Building features file...")
        for obj in tweets:
            self.count += 1
            self.write(obj)
            if self.count % 100 == 0:
                logging.info("{} elements retrieved".format(self.count))
        end = time.time()
        logging.info("Total of {0} elements retrieved in {1} seconds".format(self.count, end - start))

    def write(self, data):
        with open(self.current_file, "a+", encoding='utf-8') as f:
            if self.line_count == 0:
                f.write("\"id\",\"nb_follower\",\"nb_following\",\"verified\",\"reputation\",\"age\",\"nb_tweets\",\"posted_at\","
                        "\"text\",\"length\",\"orthographe\",\"nb_urls\",\"nb_hashtag\",\"nb_emoji\",\"named_id\",\"type\",\"spam\"\n")
            f.write(
                data["id_str"] +
                self.user_features(data) +
                self.information_contenu(data) +
                "," + data["type"] +","+ str(data["spam"]) +"\n")
        self.line_count += 1

        if self.line_count > FILEBREAK:
            logging.info("Closing file {}".format(self.current_file))
            self.current_file = FILEDIR + "data_dac2.csv"
            self.line_count = 0

    def retrievefromlist(self, listpath):
        with open(FILEDIR + listpath, 'r') as f:
            reader = csv.reader(f)
            tweet_list = list(reader)
        print(tweet_list[0])
        start = time.time()
        logging.info("Retrieving data...")
        tweets = self.db.tweets.find({"id_str": { "$in": tweet_list[0] }})
        logging.info("Building features file...")
        for obj in tweets:
            self.count += 1
            self.write(obj)
            if self.count % 100 == 0:
                logging.info("{} elements retrieved".format(self.count))
        end = time.time()
        logging.info("Total of {0} elements retrieved in {1} seconds".format(self.count, end - start))

    @staticmethod
    def user_features(data):
        user = data["user"]
        created_at = datetime.strptime(user["created_at"], '%a %b %d %H:%M:%S %z %Y')
        now = datetime.now(timezone.utc)
        age = (now - created_at).days
        if user["followers_count"] + user["friends_count"] > 0:
            reputation = user["followers_count"]/(user["followers_count"] + user["friends_count"])
        else:
            reputation = 0
        ts = int(data["timestamp_ms"])/1000
        posted_at = datetime.utcfromtimestamp(ts).strftime('%H:%M:%S')
        result = "," + str(user["followers_count"])
        result += "," + str(user["friends_count"])
        result += "," + ("1" if user["verified"] else "0")
        result += "," + ("%.2f" % round(reputation, 2))
        result += "," + str(age)
        result += "," + str(user["statuses_count"])
        result += "," + "\"" + posted_at + "\""
        return result

    @staticmethod
    def information_contenu(data):
        message = data['text'].lower()
        doc = nlp (message)
        #On récupère une liste de tous les mots qui composent les tweet et on les compare au dictionnaire pour voir s'ils sont bien orthographies/existent
        liste = [str(token) for token in doc]
        spell_dict = enchant.Dict('fr_FR')
        mot_bien_orth = 0
        for mot in liste:
            if spell_dict.check(mot):
                mot_bien_orth += 1
        ratio_orth = mot_bien_orth/len(liste)

        #On compte le nombre d'emoji dans le tweet
        emojiList = emojilist
        emoji = 0
        for j in emojiList:
            if j in message:
                emoji += 1

        result = ",\"" + str(data['text']) + "\""
        result += "," + str(len(data['text']))
        result += "," + ("%.2f" % round(ratio_orth, 2))
        result += "," + str(len(data['entities']['urls']))
        #On compte le nb de hashtag
        result += "," + str(message.count('#'))
        result += "," + str(emoji)
        #On récupère le nombre d'entites nommees
        result += "," + str(len(doc.ents))
        return result


if __name__ == "__main__":
    features = FeaturesBuilder()
    features.retrieve()
