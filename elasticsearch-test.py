from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, document, field, connections, Q
from elasticsearch_dsl.connections import connections
import codecs
import glob
import json
from prettyprint import pp


class Book(document.DocType):
    # authors = field.Nested(properties={'name': field.String(),'url':field.String(index='not_analyzed')})
    authors_name = field.String()
    authors_url = field.String(index='not_analyzed')
    average = field.Float()
    cover = field.String(index='not_analyzed')
    description = field.String()
    outlinks = field.String(index='not_analyzed')
    ratings = field.Integer()
    reviews = field.Integer()
    title = field.String()
    url = field.String(index='not_analyzed')
    # userreviews = field.Nested(properties={'userName': field.String(), 'userReview': field.String(), 'userReviewDate': field.String(index='not_analyzed'), 'userURL': field.String(index='not_analyzed')})
    userreviews_userName = field.String()
    userreviews_userReview = field.String()
    userreviews_userReviewDate = field.String(index='not_analyzed')
    userreviews_userURL = field.String(index='not_analyzed')

    def add_authors(self, authors):
        self.authors_name = [author['name'] for author in authors]
        self.authors_url = [author['url'] for author in authors]
    def add_userreviews(self, reviews):
        self.userreviews_userName = [rev['userName'] for rev in reviews]
        self.userreviews_userReview = [rev['userReview'] for rev in reviews]
        self.userreviews_userReviewDate = [rev['userReviewDate'] for rev in reviews]
        self.userreviews_userURL = [rev['userURL'] for rev in reviews]

    class Meta:
        doc_type = 'book'
        index = 'book-index'

if __name__ == '__main__':
    es = Elasticsearch()
    es.indices.create(index='book-index', ignore=[400, 404])
    connections.create_connection(hosts=['localhost'], timeout=20)
    connections.add_connection('book', es)
    print(connections.get_connection().cluster.health())

    all_json_dirs = glob.glob('JSONs/*.json')
    all_jsons = []
    for jdir in all_json_dirs[:10]:
        with open(jdir, 'r') as f:
            jsn = json.load(f)
            all_jsons.append(jsn)
    print len(all_jsons)

    Book.init('book-index')
    for idx, js in enumerate(all_jsons):
        book = Book(average=js['average'], cover=js['cover'], description=js['description'].encode('utf-8'), ratings=js['ratings'], reviews=js['reviews'], title=js['title'], url=js['url'], outlinks=js['outlinks'])
        book.add_authors(js['authors'])
        book.add_userreviews(js['userreviews'])
        # book.authors_url = authors_urls
        # for userrev in js['userreviews']:
        #     book.add_userreview(userrev['userName'], userrev['userReview'], userrev['userReviewDate'], userrev['userURL'])
        book.id = idx
        book.save()






    # s = Search(es).index('book-index').doc_type('book').query("match", description='prince')
    s = Search(es).index('book-index').doc_type('book')

    response = s.execute()
    print response.success(), response.hits.total
    for res in response:
        print res._meta.score
        print res.title
        print res.description.encode('utf-8')
        # pp(res.outlinks)
        pp(res.userreviews_userName)
        pp(res.userreviews_userReview)
        print ''

    # print response.to_dict()
