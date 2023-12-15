import redis
import mechanicalsoup
import configparser 
from elasticsearch import Elasticsearch, helpers
from neo4j import GraphDatabase
import sqlite3

# driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', '01010101'))
URI = 'neo4j://localhost:7687'
AUTH = ('neo4j', '01010101')


conn = sqlite3.connect('developers.db')
cursor = conn.cursor()

# Create a table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS table_data (
        id INTEGER PRIMARY KEY,
        technology TEXT,
        developer TEXT
    )
''')
conn.commit()


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

# start_node("test1")
# add_node("test1", "test2")

config = configparser.ConfigParser()
config.read('elastic.ini')
print(config.read('elastic.ini'))

# es = Elasticsearch(
#     cloud_id= config['ELASTIC']['cloud_id'],
#     http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
# )

# def write_to_elastic(es, url, html):
#     link = url.decode('utf-8')
#     es.index(
#         index= 'webpages', 
#         document={
#             'url': link,
#             'html': html
#         }
#     )

def crawl(browser, r, url):
    print(url)
    browser.open(url)

    try:
        title_tag = browser.page.select_one('h1.firstHeading').text
        developer_head = browser.page.find('th', text='Developer')
        developer_body = developer_head.find_next_sibling('td').get_text()
        print(developer_body)
        cursor.execute('''
                INSERT INTO table_data (technology, developer)
                VALUES (?, ?)
            ''', (title_tag, developer_body))
        conn.commit()
    except:
        print("Page missing some info needed")

    #cache to elastic 
    # write_to_elastic(es, url, str(browser.page))

    #get links
    links = browser.page.find_all("a")
    hrefs = [a.get("href") for a in links]

    # Do filtering
    domain = "https://en.wikipedia.org"
    links = [domain + a for a in hrefs if a and a.startswith("/wiki/")]
    print("url start" + str(url))
    # for i in links:

        # add_node(str(url), str(i))

    print("pushing links to redis")
    r.lpush("links", *links)

start_url = "https://en.wikipedia.org/wiki/Swift_(programming_language)"
# start_node(start_url)
r = redis.Redis()
browser = mechanicalsoup.StatefulBrowser()

r.lpush("links", start_url)
while link := r.rpop("links"):
    # start_node(str(link))
    crawl(browser, r, link)
