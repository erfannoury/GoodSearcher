from scraper import Scraper
import urllib
from bs4 import BeautifulSoup as bs
from prettyprint import pp

addr = 'JSONs'
urls = ['http://www.goodreads.com/book/show/3241368-the-little-prince-letter-to-a-hostage',
        'http://www.goodreads.com/book/show/72212.The_Kingdom_by_the_Sea',
        'http://www.goodreads.com/book/show/31626.The_Gift_of_the_Magi_and_Other_Short_Stories',
        'http://www.goodreads.com/book/show/823068.Shh_We_re_Writing_the_Constitution'
        ]
for url in urls:
    sc = Scraper(url)
    sc.writeJSON(addr)
