# encoding: utf-8
import requests
import json
from modules.resultset import ResultSet

class Topic(object):

    def __init__(self, topic_id, lang="sv"):
        """
            :param topic_id: eg. "BE/BE0101/BE0101F/UtlmedbTotNK"
            :type topic_id: str
            :param lang: "se"|"en"
        """
        self.id = topic_id
        self.url = "http://api.scb.se/OV0104/v1/doris/{}/ssd/{}"\
            .format(lang, topic_id)

        self._metadata = None
        self._dimensions = None

    @property
    def metadata(self):
        """ Get metadata about topic
        """
        if self._metadata is None:
            r = requests.get(self.url)
            # TODO: Handle errors
            self._metadata = r.json()

        return self._metadata

    @property
    def label(self):
        """ Get label of topic (called "title" by SCB)
        """
        return self.metadata["title"]

    @property
    def dimensions(self):
        """ Get a list of dimensions (called "variables" by SCB)
            :rtype: Dimension
        """
        if self._dimensions is None:
            self._dimensions = [
                Dimension(x) for x in self.metadata["variables"]
            ]
        return self._dimensions

    @property
    def content_dimensions(self):
        """ Get a list of all content dimensions
        """
        return self._get_dim_by_type("content")

    @property
    def regions(self):
        """ Get the regional dimension (if any)
        """
        try:
            return self._get_dim_by_type("region")[0]
        except IndexError:
            raise ValueError(u"{} has no regional dimension"\
                .format(self.id))



    def dimension(self, dim_id):
        """ Get dimension by id
        """
        try:
            return [x for x in self.dimensions if x.id == dim_id][0]
        except IndexError:
            raise KeyError("No dimension named '{}' in '{}'."\
                .format(dim_id, self.id))


    def query(self, *args, **kwargs):
        """ Make a query on this topic
            :param query: dict or path to dict with SCB query
        """
        query = Query(self)
        if len(args) == 1:
            query_src = args[0]
            if isinstance(query_src, str) or isinstance(query_src, unicode):
                # From file
                res = query.from_file(query_src)
            else:
                # With json
                res = query.with_json(query_src)

        else:
            res = query.with_params(kwargs)

        return res

    def _get_dim_by_type(self, dim_type):
        """ Get all dimensions of a given type
            :param dim_type: "region"|"time"|"other"|"content"
        """
        return [
            x for x in self.dimensions if x.type == dim_type
        ]


class Dimension(object):
    def __init__(self, dim_json):
        """
            :dim_json: JSON data from `metadata.variables`
        """
        self.json = dim_json

        # Parse categories
        self._categories = {}
        for i, id_ in enumerate(self.json["values"]):
            label = self.json["valueTexts"][i]
            self._categories[id_] = Category(id_, label)

    def __repr__(self):
        return u"{} ({})"\
            .format(self.label, self.id)\
            .encode("utf-8")

    @property
    def id(self):
        """ Get id (called "code" by SCB)
        """
        return self.json["code"]

    @property
    def label(self):
        """ Get label (called "text" by SCB)
        """
        return self.json["text"]

    @property
    def note(self):
        """ Get note (called "comment") by SCB
        """
        try:
            return self.json["comment"]
        except KeyError:
            return None

    @property
    def type(self):
        """ Get column type
            returns: "region"|"time"|"other"|"content"
        """
        if self.id == "Region":
            return "region"
        elif self.id == "Tid":
            return "time"
        elif self.id == "ContentsCode":
            return "content"
        else:
            return "category"


    @property
    def categories(self):
        """ Get a list of all categories for the dimensio
        """
        return self._categories.values()

    def category(self, cat_id):
        """ Get category by id
        """
        try:
            return self._categories[cat_id]
        except KeyError:
            raise KeyError(u"No category named '{}' in '{}'"\
                .format(cat_id, self.id))

class Category(object):
    def __init__(self, id_, label):
        self.id = id_
        self.label = label

    def __repr__(self):
        return u"{} ({})"\
            .format(self.label, self.id)\
            .encode("utf-8")


class Query(object):
    """ Represents a json query object to be posted to the API
    """

    def __init__(self, topic):
        """ :param topic: the topic in which the query is made
            :type topic: Topic
        """
        self.topic = topic
        self._json = None

        # Max number of values in query
        self.query_limit = 100000

    @property
    def json(self):
        """ JSON representation of the query (as passed to the API)
        """
        return self._json


    def with_json(self, json_query):
        return self._query(json_query)

    def from_file(self, file_path):
        with open(file_path) as f:
            json_query = json.load(f)
            return self._query(json_query)

    def with_params(self, params):
        json_query = {
            "query": [],
            "response": {
                "format": "json"
            }
        }
        for dim_id, values in params.iteritems():
            json_query["query"].append({
                "code": dim_id,
                "selection": {
                    "filter": "item",
                    "values": values
                }
            })

        return self._query(json_query)

    @property
    def size(self):
        if self.json is None:
            return None

        values = [x["selection"]["values"] for x in self.json["query"]]
        size = reduce(lambda x,y: x*y, [len(l) for l in values])
        return size


    def _query(self, json_query):
        """ Perform the actual query
        """
        self._json = json_query
        self._validate_query(json_query)

        if self.size > self.query_limit:
            raise NotImplementedError()

        r = requests.post(self.topic.url, json=json_query)
        if r.status_code == 404:
            raise Exception("404: Invalid query")


        return ResultSet(r.json(), self)



    def _validate_query(self, json_query):
        """ Make sure that the query contains valid values
        """
        for x in json_query["query"]:
            dim_id = x["code"]
            values = x["selection"]["values"]
            dim = self.topic.dimension(dim_id)
            cat_ids = [x.id for x in dim.categories]
            invalid_cats = list(set(values) - set(cat_ids))
            if len(invalid_cats) > 0:
                msg = u"{} are not valid values for dimension '{}'."\
                    .format(invalid_cats, dim_id)
                raise ValueError(msg)

        return True





