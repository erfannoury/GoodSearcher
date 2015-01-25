import argparse
import web
import subprocess
urls = (
    '/(.+)', 'index',
    '/', 'search'
)

use_elasticsearch = True

class search:

    def GET(self):
        render = web.template.render('templates/')
        return render.search()


class index:

    def GET(self, query):
        data_input = web.input()
        page = 0
        if "page" in data_input:
            page = int(data_input["page"])
        render = web.template.render('templates/')
        anses = []

        if use_elasticsearch:
            # importing libraries for Elasticsearch
            from elasticsearch import Elasticsearch
            from elasticsearch_dsl import Search, document, field, connections, Q
            from elasticsearch_dsl.connections import connections
            from booktype import Book

            es = Elasticsearch()
            es.indices.create(index='book-index', ignore=[400, 404])
            connections.create_connection(hosts=['localhost'], timeout=20)
            connections.add_connection('book', es)
            # print(connections.get_connection().cluster.health())
            s = Search(es).index('book-index').doc_type('book').query(Q('match', title=query.strip()) | Q('match', description=query.strip()))
            ## This damn statement took half an hour from me! Nowhere in the documentation indicated that this statement should be before s.execute()
            s = s[page*10 : page * 10 + 10]
            response = s.execute()
            # print 'total number of hits: ', response.hits.total
            num_pages = (response.hits.total / 10) + 1
            for res in response:
                authors = zip(res.authors_name, res.authors_url)
                anses.append({'title':res.title, 'description': res.description.encode('utf-8'), 'url': res.url, 'cover':res.cover, 'authors':authors})
        else:
            # importing libraries for Lucene
            pass
        return render.index(anses, query, num_pages)

if __name__ == "__main__":
    """
    main entry of the searching module
    """
    if use_elasticsearch:
        'Searching using Elasticsearch index.'
    else:
        'Searching using Lucene index.'
    app = web.application(urls, globals())
    app.run()
