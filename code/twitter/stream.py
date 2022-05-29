import re
import subprocess
import time
import tweepy
import emoji
import os
import codecs
import certifi
from dotenv import load_dotenv , find_dotenv
from pymongo import MongoClient
import pandas as pd
import spacy
import stanza


# set up a new class using tweepy.StreamListener
class SimpleListener(tweepy.Stream):
    client = MongoClient("mongodb+srv://user:XSVUTDhgT68kNZp@cluster0.nf86w.mongodb.net/Twitter-dbs?retryWrites=true&w=majority", tlsCAFile=certifi.where())
    db = client['collected_tweets']
    collection = db['tweet_stream']

    # Method that will be executed every time the listener gets a tweet matching with the query specified on main. Its main purpose is to connect with the database
    # and insert all the interesting field from every tweet that calls this method.
    def on_status(self, status):

        tweet_id = status.id_str


        if not hasattr(status, "retweeted_status"):  # Check if it is not a Retweet
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


# Method to clean the text for analyzing it. It strips it from emojis, symbols, links, hashtags and mentions, it also normalizes it.
def clean_text(text):
    c_t = re.sub(emoji.get_emoji_regexp(), " ", text)
    c_t = re.sub("(@.+)|(#.+)•", "", c_t)
    c_t = re.sub(r"https\S+", "", c_t)
    c_t = re.sub(r'[^\w]', ' ', c_t)
    c_t = c_t.lower()
    return " ".join(c_t.split())


# Method that is used to format the text before sending it to IPFS
def clean_text_final_format(text):
    c_t = re.sub(r'\n', ' ', text)
    c_t = re.sub(r'"', '\\"', c_t)

    return " ".join(c_t.split())

# Method that based on the algorithm determines whether a given text is a complain or not
def is_a_complain(text, freq_dict):
    value = 0
    repeated_words = []

    for i in range(len(freq_dict)):
        if freq_dict["WORD"][i] in text and freq_dict["WORD"][i] not in repeated_words:
            value += 1
            repeated_words.append(freq_dict["WORD"][i])

    return ((value / len(freq_dict)) >= 0.0534)
    # ---------- PARA HACER PRUEBAS ----------
    # return ((value / len(freq_dict)) > 0)


# Method which given a tweet cleans its text and analyzes it, determines if it is a complaint and in that case writes a JSON version
# of the tweet with most important fields
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
        aux_json += "{\"link\":\"" + post['link'] + "\", \"id\":\"" + post['id'] + "\", \"text\":\"" + clean_text_final_format(post["text"]) + "\", \"user\":\"" + post['user'] + "\", \"date\":"\
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

# Here will be run the other thread which main purpose will be to delete every tweet from de database everytime it has finished
# processing whether it was a complaint or not. When we have 20 we close the file of JSONs and send it to the API. Then we start over again.
def main():

    query = pd.read_csv("../../dict/query_dic.csv")
    freq_dict = pd.read_csv("../../dict/FREQUENCIES_DIC.csv")
    load_dotenv(find_dotenv("env/TwitterTokens.env"))
    tweepy_stream = SimpleListener(os.getenv('API_KEY'), os.getenv('API_KEY_SECRET'), os.getenv('ACCESS_TOKEN'), os.getenv('ACCESS_TOKEN_SECRET'), daemon=True)
    tweepy_stream.filter(languages=['es'], threaded=True, track=[query["WORD"][0], query["WORD"][1], query["WORD"][2], query["WORD"][3], query["WORD"][4], query["WORD"][5],
                                                  query["WORD"][6],query["WORD"][7],query["WORD"][8],query["WORD"][9],query["WORD"][10],query["WORD"][11],
                                                  query["WORD"][12],query["WORD"][13],query["WORD"][14],query["WORD"][15],query["WORD"][16],query["WORD"][17],
                                                  query["WORD"][18],query["WORD"][19],query["WORD"][20],query["WORD"][21],query["WORD"][22],query["WORD"][23],
                                                  query["WORD"][24]])

    f = codecs.open("../../json/examples.json", 'a+', encoding='utf-8', errors='ignore')
    f.write("[")

    client = MongoClient("mongodb+srv://user:XSVUTDhgT68kNZp@cluster0.nf86w.mongodb.net/Twitter-dbs?retryWrites=true&w=majority", tlsCAFile=certifi.where())
    db = client['collected_tweets']
    collection = db['tweet_stream']

    nlp = spacy.load("es_core_news_sm")
    nlp_s = stanza.Pipeline(lang='es', processors='tokenize,mwt,pos,lemma')
    index = 0

    try:
        while (1):
            for post in collection.find():
                if(index > 80):
                    erase_lastjson(f)
                    f.write("]")
                    f.close()
                    p = subprocess.Popen(["node", "api1.js"])
                    p.wait()
                    f = codecs.open("../../json/examples.json", 'a+', encoding='utf-8', errors='ignore')
                    f.seek(0, os.SEEK_SET)
                    f.truncate()
                    index = 0;
                    f.write("[")

                if text_analysis(post, nlp, nlp_s, freq_dict, f):
                    index+=1
                collection.delete_one({"_id": post['_id']})

    finally:

        erase_lastjson(f)

        f.write("]")
        f.close()

# Method used to delete the last JSON that has been written. This is used, because since the file is send to the API, we cannot risk
# to send something that is not in its final format. That means an incomplete JSON which was being written or if the
# execution abruptly stops, not letting something incomplete sit there. It will also delete the last comma that is always written.
def erase_lastjson(f):
    n_c = 0
    f.seek(0, os.SEEK_END)
    file_size = f.tell()
    while (file_size - n_c) > 0:
        f.seek(file_size - n_c)
        aux = f.read(file_size - n_c)
        if aux != '':
                if aux[0] == '}':
                        break
        n_c += 1

    f.seek(-n_c+1, os.SEEK_END)
    f.truncate()


if __name__ == "__main__":
    # calling main function
    main()
