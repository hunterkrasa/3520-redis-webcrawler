import redis
import mechanicalsoup
import configparser 
from elasticsearch import Elasticsearch, helpers
from neo4j import GraphDatabase

# driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', '01010101'))
URI = 'neo4j://localhost:7687'
AUTH = ('neo4j', '01010101')

def start_node(page):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        summary = driver.execute_query(
        "MERGE (:Link {name: $link})",  
        link=page,  
        database_="webcrawler",  
        ).summary
        print("Created {nodes_created} nodes in {time} ms.".format(
            nodes_created=summary.counters.nodes_created,
            time=summary.result_available_after
        ))

def add_node(page, link):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        records, summary, keys = driver.execute_query("""
        MATCH (p:Link {name: $page})
        MERGE (p)-[:Links]->(:Link {name: $link})
        """, page=page, link=link,
        database_="webcrawler",
        )
        print(f"Query counters: {summary.counters}.")

start_node("test1")
add_node("test1", "test2")

config = configparser.ConfigParser()
config.read('elastic.ini')
print(config.read('elastic.ini'))

es = Elasticsearch(
    cloud_id= config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

def write_to_elastic(es, url, html):
    link = url.decode('utf-8')
    es.index(
        index= 'webpages', 
        document={
            'url': link,
            'html': html
        }
    )

def crawl(browser, r, es, url):
    print(url)
    browser.open(url)

    #cache to elastic 
    write_to_elastic(es, url, str(browser.page))

    #get links
    links = browser.page.find_all("a")
    hrefs = [a.get("href") for a in links]

    # Do filtering
    domain = "https://en.wikipedia.org"
    links = [domain + a for a in hrefs if a and a.startswith("/wiki/")]
    print("url start" + str(url))
    for i in links:

        add_node(str(url), str(i))

    print("pushing links to redis")
    r.lpush("links", *links)

start_url = "https://en.wikipedia.org/wiki/Redis"
# start_node(start_url)
r = redis.Redis()
browser = mechanicalsoup.StatefulBrowser()

r.lpush("links", start_url)
while link := r.rpop("links"):
    start_node(str(link))
    crawl(browser, r, es, link)
