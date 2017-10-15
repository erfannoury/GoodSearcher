GoodSearcher
============

A pyLucene-based crawl and search module for searching books from goodreads.com

![Image of GoodSearcher homepage](/homepage.png)


| Module  | Files | Description|
|-------|--------|-------------|
|Crawling|crawler.py, scraper.py, SetQueue.py|These files contain the implementation of a multi-threaded crawler. Specifically, scraper.py is the implementation for scraping webpages from Goodreads website. crawler.py implements the crawler threads, thread-safe url queues and related stuff. A thread-safe queue with a built-in set for url deduplication is implemented in SetQueue.py|
|Indexing|booktype.py, indexer.py, PageRank.py| Indexing using two libraries has been implemented. You can choose between Elasticsearch and Lucene for indexing. For document boosting, PageRank scoring is also implemented.|
|Searching|searcher.py| It is a web.py-based minimal web server for searching indexes and displaying results in a web page.|

## Libraries used
#### Elasticsearch
* [Elasticsearch](https://github.com/elasticsearch/elasticsearch): Distributed, RESTful Search Engine
* [Elasticsearch-py](https://github.com/elasticsearch/elasticsearch-py): Official Python low-level client for Elasticsearch
* [Elasticsearch-dsl-py](https://github.com/elasticsearch/elasticsearch-dsl-py): High level Python client for Elasticsearch

#### Lucene
* [PyLucene](http://lucene.apache.org/pylucene/): PyLucene is a Python extension for accessing Java Lucene.
* [PyLucene](https://github.com/svn2github/pylucene): PyLucene clone on Github

#### Web
* [Web.py](http://webpy.org/): web.py is a web framework for Python that is as simple as it is powerful.
* [Beautifulsoup4](http://www.crummy.com/software/BeautifulSoup/): For web scraping

Scipy/Numpy was also used for calculating PageRank scores.
