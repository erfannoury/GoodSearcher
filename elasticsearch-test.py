from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, document, field, connections, Q
from elasticsearch_dsl.connections import connections
import codecs
import glob
import json


class Book(document.DocType):
    authors = field.Nested(properties={'name': field.String(),'url':field.String(index='not_analyzed')})
    average = field.Float()
    cover = field.String(index='not_analyzed')
    description = field.String()
    # outlinks = Nested(properties={'outlink': String(index='not_analyzed')})
    outlinks = field.String(index='not_analyzed')
    ratings = field.Integer()
    reviews = field.Integer()
    title = field.String()
    url = field.String(index='not_analyzed')
    userreviews = field.Nested(properties={'userName': field.String(), 'userReview': field.String(), 'userReviewDate': field.String(index='not_analyzed'), 'userURL': field.String(index='not_analyzed')})

    class Meta:
        doc_type = 'book'
        index = 'book-index'

    def add_author(self, name, url):
        self.authors.append({'name': name, 'url': url})

    def add_userreview(self, username, userreview, userreviewdate, userurl):
        self.userreviews.append({'userName': username, 'userReview': userreview, 'userReviewDate': userreviewdate, 'userURL': userurl})

    # def add_outlinks(self, outlink):
    #     self.outlinks.append({'outlink': outlink})

    # def save(self, ** kwargs):
    #     return super().save(** kwargs)

if __name__ == '__main__':
    es = Elasticsearch()
    es.indices.create(index='book-index', ignore=[400, 404])
    connections.create_connection(hosts=['localhost'], timeout=20)
    connections.add_connection('book', es)
    print(connections.get_connection().cluster.health())

    all_json_dirs = glob.glob('JSONs/*.json')
    all_jsons = []
    for jdir in all_json_dirs:
        with open(jdir, 'r') as f:
            jsn = json.load(f)
            all_jsons.append(jsn)
    print len(all_jsons)

    Book.init('book-index')
    for idx, js in enumerate(all_jsons):
        book = Book(average=js['average'], cover=js['cover'], description=js['description'].encode('utf-8'), ratings=js['ratings'], reviews=js['reviews'], title=js['title'], url=js['url'], outlinks=js['outlinks'])
        for author in js['authors']:
            book.add_author(author['name'], author['url'])

        for userrev in js['userreviews']:
            book.add_userreview(userrev['userName'], userrev['userReview'], userrev['userReviewDate'], userrev['userURL'])
        book.id = idx
        book.save()






    s = Search(es).index('book-index').doc_type('book').query("match", description='prince')

    response = s.execute()
    print response.success(), response.hits.total
    for res in response:
        print res._meta.score
        print res.title
        print res.description.encode('utf-8')
        print ''

    # print response.to_dict()
