from elasticsearch_dsl import document, field

class Book(document.DocType):
    """
    This is the document type class for objects of type book for using with Elasticsearch DSL using Python
    """
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
    userreviews_userName = field.String()
    userreviews_userReview = field.String()
    userreviews_userReviewDate = field.String(index='not_analyzed')
    userreviews_userURL = field.String(index='not_analyzed')

    class Meta:
        """
        meta data for the Book document type
        """
        doc_type = 'book'
        index = 'book-index'

    def add_authors(self, authors):
        """
        This method will add all of the authors from the authors list of dicts to Book
        authors = json['authors']
        """
        self.authors_name = [author['name'] for author in authors]
        self.authors_url = [author['url'] for author in authors]

    def add_userreviews(self, reviews):
        """
        This method will add all of the user reviews from the user reviews list of dicts to Book
        reviews = json['userreviews']
        """
        self.userreviews_userName = [rev['userName'] for rev in reviews]
        self.userreviews_userReview = [rev['userReview'] for rev in reviews]
        self.userreviews_userReviewDate = [rev['userReviewDate'] for rev in reviews]
        self.userreviews_userURL = [rev['userURL'] for rev in reviews]
