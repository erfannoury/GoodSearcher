import json
import glob
import numpy as np
import prettyprint as pp
import codecs
import os
import argparse

from PageRank import Normalize, PageRankScores
from booktype import Book

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, document, field, connections, Q
from elasticsearch_dsl.connections import connections

import lucene
from java.io import File
from org.apache.lucene.index import IndexWriterConfig, IndexWriter, FieldInfo
from org.apache.lucene.document import Document, Field, FieldType, IntField, FloatField
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.analysis.standard import StandardAnalyzer



def main(use_elasticsearch = True, calculate_PageRank = False, tele_const = 0.2):
    """
    main entry for the indexer module.
    """
    jsons_root_dir = 'JSONs/'

    # list of addresses of all json files
    all_json_dirs = glob.glob(jsons_root_dir + '*.json')

    # first reading all json files
    jsons = []
    for jdir in all_json_dirs:
        with open(jdir, 'r') as f:
            jsn = json.load(f)
            jsons.append(jsn)
    print len(jsons), ' json files imported.'

    # now creating a set of all links and then a list of all links in json files
    print 'creating a list of all links'
    links_set = set()
    for js in jsons:
        links_set.add(js["url"])
        for l in js["outlinks"]:
            links_set.add(l)
    print len(links_set), ' links found'
    links = list(links_set)

    ## if user has selected to index documents using Elasticsearch
    # Note that when using Elasticsearch, page rank is ignored
    if use_elasticsearch:
        print 'Using Elasticsearch for indexing, PageRank is ignored'
        es = Elasticsearch()
        es.indices.create(index='book-index', ignore=[400, 404])
        connections.create_connection(hosts=['localhost'], timeout=20)
        connections.add_connection('book', es)
        Book.init('book-index')

        ## adding all document to the index 'book-index'
        for idx, js in enumerate(jsons):
            book = Book(average=js['average'], cover=js['cover'], description=js['description'].encode('utf-8'), ratings=js['ratings'], reviews=js['reviews'], title=js['title'], url=js['url'], outlinks=js['outlinks'])
            book.add_authors(js['authors'])
            book.add_userreviews(js['userreviews'])
            book.id = idx
            book.save()
        print 'Elasticsearch index created'

    ### use pyLucene instead
    else:
        print 'Using Lucene for indexing'
        ## if user has selected to calculate the PageRank
        if calculate_PageRank:
            # now creating the unnormalized adjacency matrix
            print 'creating the unnormalized adjacency matrix.'
            adjacency = np.zeros((len(links_set), len(links_set)))
            for js in jsons:
                node_idx = links.index(js["url"])
                for l in js["outlinks"]:
                    out_idx = links.index(l)
                    adjacency[node_idx, out_idx] += 1
            print 'the unnormalized adjacency matrix created.'

            print 'normalizing the adjacency matrix with teleporting constant value of ', tele_const
            norm_mat = Normalize(adjacency, tele_const)
            print 'calculating the PageRank scores'
            pr_scores = PageRankScore(norm_mat)

        ## here goes the pyLucene code, which means I should swith to the damn Ubuntu
        index_folder = '.'
        index_name = 'lucene.index'
        index_path = os.path.join(index_folder, index_name)
        print 'initializing Lucene VM'
        lucene.initVM()
        print 'lucene version ', lucene.VERSION
        version = Version.LUCENE_CURRENT
        index_store = SimpleFSDirectory(File(index_path))
        analyzer = StandardAnalyzer(version)
        config = IndexWriterConfig(version, analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        writer = IndexWriter(index_store, config)


        # Options
        TokenizeFields = True


        # Title field type
        title_field = 'title'
        tft = FieldType()
        tft.setIndexed(True)
        tft.setStored(True)
        tft.setTokenized(TokenizeFields)
        tft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS) #only index the document and frequency data

        # Authors name field type
        authors_name_field = 'authors_name'
        anft = FieldType()
        anft.setIndexed(True)
        anft.setStored(True)
        anft.setTokenized(TokenizeFields)
        anft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS)

        # Authors url field type
        authors_url_field = 'authors_url'
        auft = FieldType()
        auft.setIndexed(False)
        auft.setStored(True)

        # Average rating field type
        average_field = 'average'

        # Cover Image URL field type
        cover_field = 'cover'
        cft = FieldType()
        cft.setIndexed(False)
        cft.setStored(True)

        # Book description field type
        description_field = 'description'
        descft = FieldType()
        descft.setIndexed(True)
        descft.setStored(True)
        descft.setTokenized(TokenizeFields)
        descft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        # Outlinks field type
        outlinks_field = "outlinks"
        outft = FieldType()
        outft.setIndexed(False)
        outft.setStored(True)

        # Ratings count field type
        ratings_field = 'ratings'

        # Reviews count field type
        reviews_field = 'reviews'

        # URL field type
        url_field = 'url'
        uft = FieldType()
        uft.setIndexed(False)
        uft.setStored(True)

        # userreviews.userName field type
        userreviews_userName_field = 'userreviews_userName'
        usunft = FieldType()
        usunft.setIndexed(False)
        usunft.setStored(True)

        #userreviews.userReview field type
        userreviews_userReview_field = 'userreviews_userReview'
        usurft = FieldType()
        usurft.setIndexed(True)
        usurft.setStored(False)
        usurft.setTokenized(TokenizeFields)
        usurft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS)

        #userreviews.userReviewDate field type
        userreviews_userReviewDate_field = 'userreviews_userReviewDate'
        usudft = FieldType()
        usudft.setIndexed(False)
        usudft.setStored(True)

        #userreviews.userURL field type
        userreviews_userURL_field = 'userreviews_userURL'
        usuuft = FieldType()
        usuuft.setIndexed(False)
        usuuft.setStored(True)

        docid_field = 'docid'




        for idx, js in enumerate(jsons):
            boostVal = js['average']
            if calculate_PageRank:
                boostVal *= pr_scores[links.index(js['url'])]
            doc = Document()
            for author in js['authors']:
                doc.add(Field(authors_name_field, author['name'], anft))
                doc.add(Field(authors_url_field, author['url'], auft))
            doc.add(FloatField(average_field, float(js['average']), Field.Store.YES))
            doc.add(Field(cover_field, js['cover'], cft))
            df = Field(description_field, js['description'], descft)
            df.setBoost(boostVal)
            doc.add(df)
            for u in js['outlinks']:
                doc.add(Field(outlinks_field, u, outft))
            doc.add(IntField(ratings_field, js['ratings'], Field.Store.YES))
            doc.add(IntField(reviews_field, js['reviews'], Field.Store.YES))
            tf = Field(title_field, js['title'], tft)
            tf.setBoost(boostVal)
            doc.add(tf)
            doc.add(Field(url_field, js['url'], uft))

            for rev in js['userreviews']:
                doc.add(Field(userreviews_userName_field, rev['userName'], usunft))
                doc.add(Field(userreviews_userReview_field, rev['userReview'], usurft))
                doc.add(Field(userreviews_userReviewDate_field, rev['userReviewDate'], usurft))
                doc.add(Field(userreviews_userURL_field, rev['userURL'], usuuft))
            doc.add(IntField(docid_field, idx, Field.Store.YES))

            writer.addDocument(doc)
        print 'lucene index created'
        writer.commit()
        writer.close()
        print 'writing lucene indexing finished'




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--lucene", action='store_true', help="Use Lucene for indexing instead of Elasticsearch.")
    parser.add_argument("-p", "--pagerank", action='store_true', help="Calculate PageRank score for documents and use this score in indexing. Ignored if Lucene isn't selected for indexing.")
    parser.add_argument("-t", "--teleporting", type=float, help="Teleporing constant (between 0 and 1). Ignored if Lucene isn't selected for indexing, or if user hasn't opted for use of PageRank scoring.")
    args = parser.parse_args()
    args.print_help()
    if not args.lucene:
        main(use_elasticsearch = True)
    else:
        if args.pagerank:
            if args.teleporting:
                main(use_elasticsearch=False, calculate_PageRank=True, tele_const=args.teleporting)
            else:
                main(use_elasticsearch=False, calculate_PageRank=True)
        else:
            main(use_elasticsearch=False, calculate_PageRank=False)
    args.print_help()
