from __future__ import unicode_literals
import urllib
from bs4 import BeautifulSoup as bs
import codecs
from fuzzywuzzy import fuzz
import json
import os
from string import punctuation, digits

class Scraper:
    """
    Goodreads books URL scraper class
    This class will get a URL and will try to scrape it and return relevant information
    """

    soup = ''
    url = ''
    # relative weighting of partial_ratio matching score and normal matching score. Partial matching is weighed more than normal matching
    pr_ratio = 0.7

    def __init__(self, __url__):
        """
        Scraper class constructor
        Webpage will be downloaded and encoded as BeautifulSoup4 object 'soup'

        Parameters
        ----------
        __url__: str
                 URL to the book webpage
        """
        self.url = __url__
        down = urllib.urlopen(self.url).read()
        self.soup = bs(down.decode('utf-8'))

    def getTitle(self):
        """
        Returns
        -------
        str: title of the book
        """
        try:
            return self.soup.find("h1", "bookTitle").text.strip()
        except:
            return ''

    def getAuthors(self):
        """
        Returns
        -------
        list(dict): a list of dict items containing author name and author page URL
                            {'name', 'url'}
        """
        authors = []
        for a in self.soup.find_all("a", "authorName"):
            authors.append({"name":a.text.strip(), "url":a.get("href")})
        return authors

    def getDescription(self):
        """
        Returns
        -------
        str: description of the book
        """
        descs = self.soup.find("div", attrs={"id":"description"}).find_all("span")
        return descs[-1].text.strip()

    def getNumbers(self):
        """
        Get average rating, total number of reviews and total number of ratings
        Returns
        -------
        dict: returns a dict object {"average": float, "ratings": int, "reviews": int}
        """
        nums = {}
        nums["average"] = float(self.soup.find("span", "average").text)
        nums["ratings"] = int(self.soup.find("span", attrs={"itemprop":"ratingCount", "class":"value-title"}).get("title"))
        nums["reviews"] = int(self.soup.find("span", "count").find("span").get("title"))
        return nums

    def getCoverPhotoURL(self):
        """
        Returns
        -------
        str: cover image url
        """
        return self.soup.find("img", attrs={"id":"coverImage"}).get("src")

    def getReviews(self):
        """
        Get user reviews of the first page (reviews of other pages may be added later)

        Returns
        -------
        list(dict): a list of user reviews as a list of dicts of the form {"userURL", "userName", "userReviewDate", "userReview"}
        """
        revs = self.soup.find("div", attrs={"id":"reviews"}).find_all("div", "friendReviews")
        reviews = []
        for rev in revs:
            review = {}
            review["userURL"] = 'http://www.goodreads.com' + rev.find("div", "review").find("a", "user").get("href")
            review["userName"] = rev.find("div", "review").find("a", "user").text
            review["userReviewDate"] = rev.find("a", "reviewDate").text
            try:
                textConts = rev.find("div", "reviewText").find("span", "readable").find_all("span")
                review["userReview"] = textConts[-1].text.strip()
            except:
                continue
            reviews.append(review)

        return reviews

    def getBookLinks(self):
        """
        Gets all the hyperlinks to Goodreads books in this webpage
        These links will be fed back to the crawler frontline queue
        Also there are the outgoing links that will be used for PageRank
        These links won't contain links related to the current URL, similar URLs will be filtered out using fuzzy string matching
        Returns
        -------
        links: list(str)
               a list of urls of the form 'http://www.goodreads.com/book/show/*'
        """
        links = []
        for link in self.soup.find_all("a"):
            l = link.get("href")
            if l:
                if l.startswith("/book/show"):
                    if l.find('?') > 0:
                        l = l[:l.find('?')]
                    l = 'http://www.goodreads.com' + l
                elif l.startswith("http://www.goodreads.com/book/show"):
                    if l.find('?') > 0:
                        l = l[:l.find('?')]
                else:
                    continue
                pr = fuzz.partial_ratio(l, self.url)
                r = fuzz.ratio(l, self.url)
                match_score = self.pr_ratio * pr + (1 - self.pr_ratio) * r
                if r < 80 and pr < 100:
                    links.append(l)

        return links

    def getJSON(self):
        """
        This will return a JSON string of the information scraped from this url
        The main object will be a dictionary and its element might themselves be objects, dictionaries, lists or anything else

        Returns
        -------
        js: str
            JSON string representation of scraped information
            Top-level object will be dictionary of this form:
            "title": str -> book title
            "authors": list(dict{"name", "url"}) -> book authors
            "description": str -> description of the book
            "average": float -> average rating of the book
            "ratings": int -> total number of user ratings for this book
            "reviews": int -> total number of user reviews for this book
            "cover": str -> URL to the photo cover
            "userreviews" -> list(dict{"userURL", "userName", "userReview", "userReviewDate"}) -> user reviews for this book
            "url": str -> this book's URL
            "outlinks" : list(str): a filtered list of outgoing URLs to other books on Goodreads website

        """
        obj = {}
        obj["title"] = self.getTitle()
        obj["authors"] = self.getAuthors()
        obj["description"] = self.getDescription()
        nums = self.getNumbers()
        obj["average"] = nums["average"]
        obj["ratings"] = nums["ratings"]
        obj["reviews"] = nums["reviews"]
        obj["cover"] = self.getCoverPhotoURL()
        obj["userreviews"] = self.getReviews()
        obj["url"] = self.url
        obj["outlinks"] = self.getBookLinks()

        return json.dumps(obj)


    def writeJSON(self, addr):
        """
        This will write the JSON output as a json file in addr directory.
        File name will be the title of the book
        """
        js = self.getJSON()
        filename = self.getTitle().decode('utf-8')
        ignoreDict = {ord(c): None for c in (punctuation + digits + ' ' + '\n')}
        filename = filename.translate(ignoreDict)
        print 'writing json file: ', filename
        with open(os.path.join(addr, filename + '.json'), "w") as f:
            f.write(js)
