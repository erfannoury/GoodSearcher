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
        num_pages = 0
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
            import lucene
            from java.io import File
            from org.apache.lucene.index import DirectoryReader, Term
            from org.apache.lucene.queryparser.classic import QueryParser
            from org.apache.lucene.store import SimpleFSDirectory
            from org.apache.lucene.search import IndexSearcher, BooleanClause, BooleanQuery, TermQuery
            from org.apache.lucene.util import Version
            from org.apache.lucene.analysis.standard import StandardAnalyzer
            import os

            # fields
            title_field = 'title'
            description_field = 'description'
            cover_field = 'cover'
            authors_name_field = 'authors_name'
            authors_url_field = 'authors_url'
            url_field = 'url'

            index_folder = '.'
            index_name = 'lucene.index'
            index_path = os.path.join(index_folder, index_name)

            lucene.initVM()
            version = Version.LUCENE_CURRENT
            directory = SimpleFSDirectory(File(index_path))
            searcher = IndexSearcher(DirectoryReader.open(directory))
            analyzer = StandardAnalyzer(version)

            title_tq = TermQuery(Term(title_field, query))
            desc_tq = TermQuery(Term(description_field, query))
            query = BooleanQuery()
            query.add(BooleanClause(title_tq, BooleanClause.Occur.SHOULD))
            query.add(BooleanClause(desc_tq, BooleanClause.Occur.SHOULD))
            scoreDocs = searcher.search(query, 1000).scoreDocs
            num_pages = (len(scoreDocs) / 10) + 1

            for scoreDoc in scoreDocs[page*10 : page * 10 + 10]:
                doc = searcher.doc(scoreDoc.doc)
                authors = zip([doc.get(authors_name_field)], [doc.get(authors_url_field)])
                anses.append({'title':doc.get(title_field), 'description': doc.get(description_field).encode('utf-8'), 'url': doc.get(url_field), 'cover':doc.get(cover_field), 'authors':authors})

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
