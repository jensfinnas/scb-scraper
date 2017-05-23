# encoding: utf-8
import json 
import pandas as pd

class ResultSet(object):
    """ Represents the result we get from a query
    """
    def __init__(self, json_data, query):
        self.json = json_data
        self.query = query
        self.topic = query.topic
        self._df = None
        self._columns = None
        self._notes = None


    @property
    def index(self):
        """ Get the id of the index columns (called "key" by SCB)
        """
        return [x.id for x in self.index_columns]

    @property
    def index_columns(self):
        return [x for x in self.columns if x.type != "content"]
    
    @property
    def columns(self):
        """ Get column ids
        """
        if self._columns is None:
            self._columns = []
            for x in self.json["columns"]:
                col = Column(
                    x["code"],
                    x["text"],
                    x["type"],
                    self)
                self._columns.append(col)
        return self._columns

    @property
    def content_columns(self):
        return [x for x in self.columns if x.type == "content"]
    
    
    @property
    def df(self):
        """ Get dataset as pandas dataframe
        """
        if self._df is None:
            data = []

            for scb_row in self.json["data"]:
                row = scb_row["key"] + scb_row["values"]
                data.append(row)

            col_ids = [x.id for x in self.columns]
            self._df = pd.DataFrame(data, columns=col_ids)\
                .set_index(self.index)

        return self._df

    def values(self, dim_id):
        """ Get the values of a given dimension
        """
        try:
            i = self.index.index(dim_id)
        except ValueError:
            raise KeyError("'{}' is not a valid dimension in {}."\
                .format(dim_id, self.topic.id))

        return self.df.index.levels[i].values

    @property
    def notes(self):
        """ Get a list of all notes related to the query
            
            :returns: a list of Note instances
        """
        if self._notes == None:
            self._notes = []
            for x in self.json["comments"]:
                note = Note(
                    x["comment"],
                    dimension=x["variable"],
                    category=x["value"])
                self._notes.append(note)

        return self._notes
    
class Column(object):
    """docstring for Column"""
    def __init__(self, id_, label, type_, resultset):
        self.id = id_
        self.label = label
        types = {
            "t": "time",
            "r": "region",
            "d": "category",
            "c": "content",
        }
        self.type = types[type_]
        self.resultset = resultset

    @property
    def values(self):
        """ Get the values of a given dimension
        """
        try:
            i = self.resultset.index.index(dim_id)
        except ValueError:
            raise KeyError("'{}' is not a valid dimension in {}."\
                .format(self.dim_id, self.resultset.topic.id))

        return self.resultset.df.index.levels[i].values
    
class Note(object):
    """ Represents a note (called "comment" by SCB)
    """ 
    def __init__(self, note, dimension=None, category=None):
        self.note = note
        self.dimension = dimension
        self.category = category