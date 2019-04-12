# -*- coding: utf-8 -*-
"""
Created on Mon Oct 29 09:35 2018

@author: dshi, hbaud, vlefranc
"""

import logging
from datetime import datetime
from config import FILEDIR, FILEBREAK, MONGODB
from pymongo import MongoClient
import time
import csv

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)


class VectorBuilder:
    """
    Retrieve data from the MongoDB database.

    """
    def __init__(self):

        self.do_continue = True
        self.count = 0
        self.line_count = 0
        self.current_file = "C:\\Users\\Public\\Documents\\data.csv"
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
            if self.count % 500 == 0:
                logging.info("{} elements retrieved".format(self.count))
        end = time.time()
        logging.info("Total of {0} elements retrieved in {1} seconds".format(self.count, end - start))

    def write(self, data):
        with open(self.current_file, "a+", encoding='utf-8') as f:
            if self.line_count == 0:
                f.write("id\s+date\s+text\s+reply to\s+url\s+user name\s+user url\s+user desc\s+verified\s+followers\s+following\s+statuses\s+age\s+spam\s+type \n")
            else :
                if data['truncated'] == True :
                    if len(data["extended_tweet"]["entities"]["urls"]) > 0 :
                        f.write(str(data["id"]) + "\s+" + str(data["created_at"]) + "\s+" + "\"" + str(data["extended_tweet"]["full_text"])
                                + "\"" + "\s+" + str(data["in_reply_to_status_id"]) +
                        "\s+" + str(data["extended_tweet"]["entities"]["urls"][0]["url"]) +
                        "\s+" + str(data["user"]["screen_name"]) + "\s+" + str(data["user"]["url"]) +
                        "\s+" + "\"" + str(data["user"]["description"]) + "\"" + "\s+" + str(data["user"]["verified"]) +
                        "\s+" + str(data["user"]["followers_count"]) +
                        "\s+" + str(data["user"]["friends_count"]) + "\s+" + str(data["user"]["statuses_count"]) +
                        "\s+" + str(data["user"]["created_at"]) +
                        "\s+" + str(data["spam"]) + "\s+" + str(data["type"]) + "\n")
                    else :
                        f.write(str(data["id"]) + "\s+" + str(data["created_at"]) + "\s+" + "\"" + str(
                            data["extended_tweet"]["full_text"])
                                + "\"" + "\s+" + str(data["in_reply_to_status_id"]) +
                                "\s+None" +
                                "\s+" + str(data["user"]["screen_name"]) + "\s+" + str(data["user"]["url"]) +
                                "\s+" + "\"" + str(data["user"]["description"]) + "\"" + "\s+" + str(
                            data["user"]["verified"]) +
                                "\s+" + str(data["user"]["followers_count"]) +
                                "\s+" + str(data["user"]["friends_count"]) + "\s+" + str(data["user"]["statuses_count"]) +
                                "\s+" + str(data["user"]["created_at"]) +
                                "\s+" + str(data["spam"]) + "\s+" + str(data["type"]) + "\n")
                else :
                    print(data["type"])
                    if len(data["entities"]["urls"])> 0 :
                        f.write(str(data["id"]) + "\s+" + str(data["created_at"]) + "\s+" + "\"" + str(
                            data["text"]) + "\"" + "\s+" + str(data["in_reply_to_status_id"]) +
                                "\s+" + str(data["entities"]["urls"][0]["url"]) +
                                "\s+" + str(data["user"]["screen_name"]) + "\s+" + str(data["user"]["url"]) +
                                "\s+" + "\"" + str(data["user"]["description"]) + "\"" +
                                "\s+" + str(data["user"]["verified"]) + "\s+" + str(data["user"]["followers_count"]) +
                                "\s+" + str(data["user"]["friends_count"]) + "\s+" + str(data["user"]["statuses_count"]) +
                                "\s+" + str(data["user"]["created_at"]) +
                                "\s+" + str(data["spam"]) + "\s+" + str(data["type"]) + "\n")
                    else :
                        f.write(str(data["id"]) + "\s+" + str(data["created_at"]) + "\s+" + "\"" + str(
                            data["text"]) + "\"" + "\s+" + str(data["in_reply_to_status_id"]) +
                                "\s+None" +
                                "\s+" + str(data["user"]["screen_name"]) + "\s+" + str(data["user"]["url"]) +
                                "\s+" + "\"" + str(data["user"]["description"]) + "\"" +
                                "\s+" + str(data["user"]["verified"]) + "\s+" + str(data["user"]["followers_count"]) +
                                "\s+" + str(data["user"]["friends_count"]) + "\s+" + str(data["user"]["statuses_count"]) +
                                "\s+" + str(data["user"]["created_at"]) +
                                "\s+" + str(data["spam"]) + "\s+" + str(data["type"]) + "\n")
        self.line_count += 1

        if self.line_count > 10000:
            logging.info("Closing file {}".format(self.current_file))
            self.current_file = FILEDIR + "tweets_" + datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") + ".csv"
            self.line_count = 0


if __name__ == "__main__":
    vect = VectorBuilder()
    vect.retrieve()
