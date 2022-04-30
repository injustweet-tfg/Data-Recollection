import snscrape.modules.twitter as sntwitter
import re
import emoji
import os
import codecs
import certifi
from dotenv import load_dotenv , find_dotenv
from pymongo import MongoClient
import pandas as pd
import spacy
import stanza
import subprocess




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
        aux_json += "{\"link\":\"" + post['link'] + "\", \"id\":\"" + post['id'] + "\", \"text\":\"" + post["text"] + "\", \"user\":\"" + post['user'] + "\", \"date\":"\
                   + str(int(post['date'].timestamp())) +", \"likes\":" + str(post['likes']) + ", \"retweets\":" + str(post['retweets']) + ", \"replies\":" + str(post['replies']) + ", \"hashtags\":"
        aux_hashtags = "["
        for h in post['hashtags']:
            aux_hashtags+= ("\"" + h + "\", ")

        if (len(aux_hashtags) > 1):
            aux_hashtags = aux_hashtags[:-2]
        aux_hashtags += "]"

        aux_json += (aux_hashtags + "}, ")
        f.write(aux_json)
        return True
    return False

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





def main():
    query = pd.read_csv("../../dict/query_dic.csv")
    freq_dict = pd.read_csv("../../dict/FREQUENCIES_DIC.csv")
    load_dotenv(find_dotenv("env/TwitterTokens.env"))

    client = MongoClient("mongodb+srv://user:XSVUTDhgT68kNZp@cluster0.nf86w.mongodb.net/Twitter-dbs?retryWrites=true&w=majority", tlsCAFile=certifi.where())
    db = client['collected_tweets']
    collection = db['tweet_scrape']

    f = codecs.open("../../json/examples_scrape.json", 'a+', encoding='utf-8', errors='ignore')

    #f = open("../../json/examples_scrape.json", 'a+')

    one_char = f.read(1)

    if not one_char:
        f.write("[")

    nlp = spacy.load("es_core_news_sm")
    nlp_s = stanza.Pipeline(lang='es', processors='tokenize,mwt,pos,lemma')


    # Using TwitterSearchScraper to scrape data and append tweets to list
    try:
        index = 0
        for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query["WORD"][0] + " OR " + query["WORD"][0] + " OR " + query["WORD"][0] + " OR " + query["WORD"][3] + " OR " + query["WORD"][4] + " OR " +
         query["WORD"][5] + " OR " + query["WORD"][6] + " OR " + query["WORD"][7] + " OR " + query["WORD"][8] + " OR " + query["WORD"][9] + " OR " +\
         query["WORD"][10] + " OR " + query["WORD"][11] + " OR " + query["WORD"][12] + " OR " + query["WORD"][13] + " OR " + query["WORD"][14] + " OR " +\
         query["WORD"][15] + " OR " + query["WORD"][16] + " OR " + query["WORD"][17] + " OR " + query["WORD"][18] + " OR " + query["WORD"][19] + " OR " +
         query["WORD"][20] + " OR " + query["WORD"][21] + " OR " + query["WORD"][22] + " OR " + query["WORD"][23] + " OR " + query["WORD"][24] + " lang:es -is:retweet").get_items()):

            if (index > 20):
                f.write("]")

                # eval_res, tempfile = js2py.run_file("api1.js")
                # tempfile.wish("GeeksforGeeks")

                p = subprocess.Popen(["node", "api2.js"])
                p.wait()

                f.seek(0, os.SEEK_SET)
                f.truncate()
                f.write("[")


            tweet_id = str(tweet.id)

            text = tweet.content
            l_hashtags = tweet.hashtags
            if l_hashtags is None:
                l_hashtags = []

            text.replace("\n", " ")
            user = tweet.user.username
            link = "https://twitter.com/" + user + "/status/" + tweet_id

            date = tweet.date
            n_likes = tweet.likeCount
            n_retweets = tweet.retweetCount
            n_replies = tweet.replyCount

            post = {'link': link, 'id': tweet_id, 'text': text, 'user': user, 'date': date, 'likes': n_likes,
                    'retweets': n_retweets, 'replies': n_replies, 'hashtags': l_hashtags}

            collection.insert_one(post)
            if (text_analysis(post, nlp, nlp_s, freq_dict, f)):
                index+=1
            collection.delete_one({"_id": post['_id']})

    finally:
        erase_lastjson(f)

        f.write("]")

        # Enviar a la api
        subprocess.Popen(["node", "api2.js"])

        f.seek(0, os.SEEK_SET)
        f.truncate()

        f.close()







if __name__ == "__main__":
    # calling main function
    main()