from update_replica_set import start_mongo_client
from datetime import datetime
from time import sleep

MIN_PRIORITY = 3
DATAMINR_MULTIPLIER = 1
NEWS_MULTIPLIER = 2
REUTERS_MULTIPLIER = 2
RECORDEDFUTURE_MULTIPLIER = 1
MAXDIFF = 172800

def adjust_priority_by_time(priority, datetime):
    if datetime > datetime.now():
        return 0
    difference = datetime.now() - datetime
    if difference.total_seconds() > MAXDIFF:
        return 0
    return priority * (MAXDIFF - difference.total_seconds()) * (MAXDIFF - difference.total_seconds())/(MAXDIFF * MAXDIFF)

def update_articles(output, client):
    try:
        to_update = client.production.articles.find({"id": output["id"]})[0]
    except Exception:
        if output["priority"] > MIN_PRIORITY:
            try:
                client.production.articles.insert(output)
            except Exception:
                pass
    else:
        output["saved"] = to_update["saved"]
        output["unseen"] = to_update["unseen"]
    if output["priority"] > MIN_PRIORITY:
        try:
            client.production.articles.replace_one({"id": output["id"]},output,upsert=True)
        except Exception:
            pass

def process_dataminr_events(raw_events, client):
    for event in raw_events:
        output = {}
        output["priority"] = 0
        tweet = event["displayTweet"]
        output["pubDate"] = datetime.utcfromtimestamp(event["eventTime"]/1000)
        output["poster"] = ""
        output["movies"] = []
        output["images"] = []
        if "media" in tweet["entities"].keys():
            maxpixels = 0
            for media in tweet["entities"]["media"]:
                if "expanded_url" in media.keys():
                    if "photo" in media["expanded_url"]:
                        output["images"].append(media["media_url_https"])
                        output["priority"] = output["priority"] + 2
                        if output["poster"] == "":
                            output["poster"] = media["media_url_https"]
                    if "video" in media["expanded_url"]:
                        output["movies"].append(media["media_url_https"])
                        output["priority"] = output["priority"] + 2
        output["title"] = tweet["user"]["screen_name"]
        output["content"] = tweet["text"]
        if "translatedText" in tweet.keys():
            output["content"] = tweet["translatedText"]
        if " CIA " in output["content"] or " CIA." in output["content"] or " CIA," in output["content"] or " CIA;" in output["content"] or "@CIA" in output["content"] or "C.I.A." in output["content"]:
            output["priority"] = output["priority"] + 50
        if "description" in tweet["user"].keys() and "name" in tweet["user"].keys():
            output["summary"] = tweet["user"]["name"]  + ": " + tweet["user"]["description"]
        elif "description" in tweet["user"].keys():
            output["summary"] = tweet["user"]["description"]
        elif "name" in tweet["user"].keys():
            output["summary"] = tweet["user"]["name"]
        else:
            output["summary"] = ""
        output["source"] = "Twitter"
        output["rawHTML"] = tweet
        output["read"] = 1
        output["topics"] = event["categories"]
        if "Crime - Criminal Activity" in output["topics"]:
            output["priority"] = output["priority"] + 1
        if "Conflicts & Violence" in output["topics"]:
            output["priority"] = output["priority"] + 2
        if "Riots & Protests" in output["topics"]:
            output["priority"] = output["priority"] + 2
        if "Disasters & Weather - Natural Disasters" in output["topics"]:
            output["priority"] = output["priority"] + 1
        if "Transportation - Traffic & Roadways" in output["topics"]:
            output["priority"] = output["priority"] - 5
        output["geos"] = []
        if "eventLocation" in event.keys():
            if "coordinates" in event["eventLocation"].keys():
                output["geos"] = [
                             event["eventLocation"]["coordinates"][1],
                             event["eventLocation"]["coordinates"][0]
                             ]
                if client.locations.embassies.find({
                             "coords": {
                               "$near": {
                                 "$geometry": {
                                   "type": "Point",
                                   "coordinates": output["geos"]
                                   },
                                 "$maxDistance": 2000
                                 }
                               }
                             }).count() > 0:
                    output["priority"] = output["priority"] + 5
                    if "Crime - Criminal Activity" in output["topics"]:
                        output["priority"] = output["priority"] + 5
                    if "Conflicts & Violence" in output["topics"]:
                        output["priority"] = output["priority"] + 10
                    if "Riots & Protests" in output["topics"]:
                        output["priority"] = output["priority"] + 10
                    if "Disasters & Weather - Natural Disasters" in output["topics"]:
                        output["priority"] = output["priority"] + 5
                if client.locations.embassies.find({
                             "coords": {
                               "$near": {
                                 "$geometry": {
                                   "type": "Point",
                                   "coordinates": output["geos"]
                                   },
                                 "$maxDistance": 500
                                 }
                               }
                             }).count() > 0:
                    output["priority"] = output["priority"] + 20
                    if "Crime - Criminal Activity" in output["topics"]:
                        output["priority"] = output["priority"] + 20
                    if "Conflicts & Violence" in output["topics"]:
                        output["priority"] = output["priority"] + 100
                    if "Riots & Protests" in output["topics"]:
                        output["priority"] = output["priority"] + 100
                    if "Disasters & Weather - Natural Disasters" in output["topics"]:
                        output["priority"] = output["priority"] + 10
        output["saved"] = "false"
        output["unseen"] = "true"
        output["story"] = []
        output["id"] = "tweet_" + str(output["rawHTML"]["id"])
        output["priority"] = adjust_priority_by_time(output["priority"], output["pubDate"])
        output["priority"] = output["priority"] * DATAMINR_MULTIPLIER
        print "Dataminr event at " + str(output["pubDate"]) + ", priority " + str(output["priority"])
        update_articles(output, client)

def is_a_in_b(a,b):
    for element in a:
        if element in b:
            return True
    return False

def process_news_events(news_events, client):
    for event in news_events:
        output = {}
        output["priority"] = 1
        try:
            output["pubDate"] = datetime.strptime(event["pubDate"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            output["pubDate"] = datetime.strptime(event["pubDate"], "%Y-%m-%d")
        output["poster"] = event["poster"]
        output["movies"] = event["movies"]
        output["images"] = event["images"]
        output["title"] = event["title"]
        output["content"] = event["content"]
        output["source"] = event["source"]
        try:
            output["rawHTML"] = "" #event["rawHTML"]
        except Exception:
            pass
        output["read"] = 3
        output["topics"] = event["tags"]
        output["geos"] = []
        output["saved"] = "false"
        output["unseen"] = "true"
        try:
            output["saved"] = event["saved"]
        except Exception:
            pass
        try:
            output["unseen"] = event["unread"]
        except Exception:
            pass
        output["story"] = []
        output["id"] = event["_id"]
        other_search_words = [" CIA "," CIA."," CIA,"," CIA;","C.I.A."]
        instability_words = ["protest","demonstration"]
        terrorism_words = ["terrorism","terrorist","explosion","bombing"," ied", "i.e.d.","drone","strike"]
        if is_a_in_b(other_search_words,output["content"]):
            output["priority"] = output["priority"] + 10
        if is_a_in_b(instability_words,output["content"]):
            output["priority"] = output["priority"] + 2
        if is_a_in_b(terrorism_words,output["content"]):
            output["priority"] = output["priority"] + 4
        if is_a_in_b(other_search_words,output["topics"]):
            output["priority"] = output["priority"] + 10
        if is_a_in_b(instability_words,output["topics"]):
            output["priority"] = output["priority"] + 2
        if is_a_in_b(terrorism_words,output["topics"]):
            output["priority"] = output["priority"] + 4
        output["priority"] = adjust_priority_by_time(output["priority"], output["pubDate"])
        output["priority"] = output["priority"] * NEWS_MULTIPLIER
        print "News event at " + str(output["pubDate"]) + ", priority " + str(output["priority"])
        update_articles(output, client)

def process_reuters_articles(reuters_articles, client):
    search_terms = []
    for article in reuters_articles:
        output = {}
        output["priority"] = 0
        output["pubDate"] = datetime.strptime(article['newsMessage']['itemSet']['newsItem']['itemMeta']['versionCreated'], "%Y-%m-%dT%H:%M:%S.000Z")
        print output["pubDate"]
        output["poster"] = ""
        output["movies"] = ""
        output["images"] = ""
        output["title"] = article['newsMessage']['itemSet']['newsItem']['contentMeta']['headline']
        output["content"] = u""
        if article['newsMessage']['itemSet']['newsItem']['contentSet']['inlineXML']['html']['body']['p'] is not None:
            for paragraph in article['newsMessage']['itemSet']['newsItem']['contentSet']['inlineXML']['html']['body']['p']:
                cleaned_paragraph = ""
                if paragraph is not None:
                    for i in paragraph:
                        if i is not None:
                            if ord(i) < 128:
                                cleaned_paragraph = cleaned_paragraph + i
                            else:
                                cleaned_paragrah = cleaned_paragraph + " "
                output["content"] = output["content"] + " " + str(cleaned_paragraph).replace("\n"," ")
        output["source"] = "Thomson Reuters"
        output["read"] = "false"
        output["topics"] = []
        output["geos"] = ""
        output["saved"] = "false"
        output["unseen"] = "true"
        output["story"] = []
        output["id"] = article["_id"]
        other_search_words = [" CIA "," CIA."," CIA,"," CIA;","C.I.A."]
        instability_words = ["protest","demonstration"]
        terrorism_words = ["terrorism","terrorist","explosion","bombing"," ied", "i.e.d.","drone","strike"]
        if is_a_in_b(other_search_words,output["content"]):
            output["priority"] = output["priority"] + 4
        if is_a_in_b(instability_words,output["content"]):
            output["priority"] = output["priority"] + 1
        if is_a_in_b(terrorism_words,output["content"]):
            output["priority"] = output["priority"] + 2
        if is_a_in_b(other_search_words,output["topics"]):
            output["priority"] = output["priority"] + 4
        if is_a_in_b(instability_words,output["topics"]):
            output["priority"] = output["priority"] + 1
        if is_a_in_b(terrorism_words,output["topics"]):
            output["priority"] = output["priority"] + 2
        output["priority"] = adjust_priority_by_time(output["priority"], output["pubDate"])
        output["priority"] = output["priority"] * REUTERS_MULTIPLIER
        print "Reuters event at " + str(output["pubDate"]) + ", priority " + str(output["priority"])
        update_articles(output, client)

if __name__ == "__main__":
    client = start_mongo_client()
    while 1:
        raw_events = client.dataminr.articles.find().sort("eventTime", -1).limit(10000)
        news_events = client.raw_articles.news.find({"pubDate": {"$ne": "None"}}).sort("pubDate", -1).limit(1000)
        reuters_events = client.tr.articles.find({"newsMessage.itemSet.newsItem.itemMeta.versionCreated": {"$exists": True}}).sort("newsMessage.itemSet.newsItem.itemMeta.versionCreated", -1).limit(1000)
        print "Processing Dataminr articles"
        process_dataminr_events(raw_events, client)
        print "Processing news articles"
        process_news_events(news_events, client)
        print "Processing Reuters articles"
        process_reuters_articles(reuters_events, client)
        client.production.articles.delete_many({"priority": {"$lt": MIN_PRIORITY} })
        sleep(5)
