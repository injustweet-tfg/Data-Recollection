import re
import tweepy
import emoji
import os
import js2py

import time

#from bson import ObjectId
from dotenv import load_dotenv , find_dotenv
from pymongo import MongoClient
import ssl
import pandas as pd
import spacy
import stanza


try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# ----------------------------------------------------     FIXED VALUES     --------------------------------------------------------------

SCREENNAME = "JobsMierda"
MOST_RECENT_ID = 0


# set up a new class using tweepy.StreamListener

class SimpleListener(tweepy.Stream):
    client = MongoClient()
    db = client['tweet_stream']
    collection = db['test']

    def on_status(self, status):

        tweet_id = status.id_str


        #if hasattr(status, "retweeted_status"):  # Check if Retweet
        #    try:
        #        text = status.retweeted_status.extended_tweet['full_text']
        #        l_hashtags = status.retweeted_status.extended_tweet['entities']['hashtags']
        #    except AttributeError:
        #        text = status.retweeted_status.text
        #         l_hashtags = status.retweeted_status.entities['hashtags']
        #else:
        if not hasattr(status, "retweeted_status"):  # Check if not Retweet
            try:
                text = status.extended_tweet["full_text"]
                l_hashtags = status.extended_tweet['entities']['hashtags']
            except AttributeError:
                text = status.text
                l_hashtags = status.entities['hashtags']

            text.replace("\n", " ")
            user = status.user.screen_name
            link = "https://twitter.com/" + user + "/status/" + tweet_id

            date = status.created_at
            n_likes = status.favorite_count
            n_retweets = status.retweet_count
            n_replies = status.reply_count


            post = {'link': link, 'id': tweet_id, 'text': text, 'user': user, 'date': date, 'likes': n_likes,
            'retweets': n_retweets, 'replies': n_replies, 'hashtags': l_hashtags}

            self.collection.insert_one(post)
            print("on_status")


    def on_error(self, staus_code):
        # code to run each time an error is received
        if staus_code == 420:
            return False
        else:
            return True


def clean_text(text):
    clean_text = re.sub(emoji.get_emoji_regexp(), " ", text)
    clean_text = re.sub("(@.+)|(#.+)•", "", clean_text)
    clean_text = re.sub(r"https\S+", "", clean_text)
    clean_text = re.sub(r'[^\w]', ' ', clean_text)
    clean_text = clean_text.lower()
    return " ".join(clean_text.split())

def is_a_complain(text, freq_dict):
    value = 0
    repeated_words = []

    for i in range(len(freq_dict)):
        if freq_dict["WORD"][i] in text and freq_dict["WORD"][i] not in repeated_words:
            value += 1
            repeated_words.append(freq_dict["WORD"][i])

    return ((value / len(freq_dict)) >= 0.0534)

def text_analysis(post, nlp, nlp_s, freq_dict,f):
    lemmatized = []
    stringed = ""
    text = clean_text(post['text'])
    obj = nlp(text)
    tokens = [tk.orth_ for tk in obj if not tk.is_punct | tk.is_stop]
    normalized = [tk.lower() for tk in tokens if len(tk) > 3 and tk.isalpha()]
    aux_json = ""

    for n in normalized:
        stringed = stringed + n + " "

    doc = nlp_s(stringed)


    for sent in doc.sentences:
        for word in sent.words:
            lemmatized.append(word.lemma)

    if(is_a_complain(lemmatized, freq_dict)):
        aux_json += "{\"link\":\"" + post['link'] + "\", \"id\":" + post['id'] + ", \"text\":\"" + text + "\", \"user\":\"" + post['user'] + "\", \"date\":"\
                   + str(int(post['date'].timestamp())) +", \"likes\":" + str(post['likes']) + ", \"retweets\":" + str(post['retweets']) + ", \"replies\":" + str(post['replies']) + ", \"hashtags\":"
        aux_hashtags = "["
        for h in post['hashtags']:
            aux_hashtags+= ("\"" + h['text'] + "\", ")

        if (len(aux_hashtags) > 1):
            aux_hashtags = aux_hashtags[:-2]
        aux_hashtags += "]"

        aux_json += (aux_hashtags + "}, \n")
        f.write(aux_json)
        return True
    return False





def main():

    query = pd.read_csv("../../dict/query_dic.csv")
    freq_dict = pd.read_csv("../../dict/FREQUENCIES_DIC.csv")
    load_dotenv(find_dotenv("env/TwitterTokens.env"))
    tweepy_stream = SimpleListener(os.getenv('API_KEY'), os.getenv('API_KEY_SECRET'), os.getenv('ACCESS_TOKEN'), os.getenv('ACCESS_TOKEN_SECRET'), daemon=True)
    tweepy_stream.filter(languages=['es'], threaded=True, track=[query["WORD"][0],query["WORD"][1],query["WORD"][2],query["WORD"][3],query["WORD"][4],query["WORD"][5],
                                                  query["WORD"][6],query["WORD"][7],query["WORD"][8],query["WORD"][9],query["WORD"][10],query["WORD"][11],
                                                  query["WORD"][12],query["WORD"][13],query["WORD"][14],query["WORD"][15],query["WORD"][16],query["WORD"][17],
                                                  query["WORD"][18],query["WORD"][19],query["WORD"][20],query["WORD"][21],query["WORD"][22],query["WORD"][23],
                                                  query["WORD"][24]])

    f = open("../../json/examples.json", 'a+')
    one_char = f.read(1)

    if not one_char:
        f.write("[")

    client = MongoClient()
    db = client['tweet_stream']
    collection = db['test']

    nlp = spacy.load("es_core_news_sm")
    nlp_s = stanza.Pipeline(lang='es', processors='tokenize,mwt,pos,lemma')
    index = 0

    try:
        while (1):
            for post in collection.find():
                if(index > 20):
                    f.write("]")

                    #eval_res, tempfile = js2py.run_file("api.js")
                    #tempfile.wish("GeeksforGeeks")

                    f.seek(0, os.SEEK_SET)
                    f.truncate()
                    f.write("[")

                if (text_analysis(post, nlp, nlp_s, freq_dict, f)):
                    index+=1
                collection.delete_one({"_id": post['_id']})




    finally:

        erase_lastjson(f)

        f.write("]")

        #Enviar a la api

        f.seek(0,os.SEEK_SET)
        f.truncate()

        f.close()

def erase_lastjson(f):
    n_c = 0
    f.seek(0, os.SEEK_END)
    file_size = f.tell()
    while (file_size - n_c) > 0:
        f.seek(file_size - n_c)
        aux = f.read(n_c)
        if (aux == '}'):
            break
        n_c += 1

    f.seek(-n_c+1, os.SEEK_END)
    f.truncate()





if __name__ == "__main__":
    # calling main function
    main()