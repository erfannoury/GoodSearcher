import json
import glob
import numpy as np
import prettyprint as pp
from PageRank import Normalize, PageRankScores
from booktype import Book
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, document, field, connections, Q
from elasticsearch_dsl.connections import connections
import codecs




def main():
    """
    main entry for the indexer part.
    """
    ##############################
    calculate_PageRank = True
    use_elasticsearch = True
    tele_const = 0.2
    ##############################

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

    # creating an inverted mapping from documents to the list of links
    for js in jsons:
        js["index"] = links.index(js["url"])
    print 'a mapping from documents to the list of links created.'

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





if __name__ == '__main__':
    main()
