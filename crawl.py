import redis
import mechanicalsoup


url = "https://www.scrapingbee.com/webscraping-questions/beautifulsoup/how-to-find-all-links-using-beautifulsoup-and-python/"
r = redis.Redis()
def recursive_scrape(url):
    print(url)
    browser = mechanicalsoup.StatefulBrowser()
    browser.open(url)
    links = browser.page.find_all("a")
    texts = browser.page.find_all(text=True)
    r.lpush('PageText', str(texts))

    for link in links:
        if "http" in link.get("href"):
            r.lpush('NikeLinks', link.get("href"))

    while(r.llen('NikeLinks')!=0):
        try:
            next_link = r.rpop("NikeLinks")
            if next_link != None and "http" in str(next_link):
                recursive_scrape(next_link)
        except:
            print("Error occured")


recursive_scrape(url)

