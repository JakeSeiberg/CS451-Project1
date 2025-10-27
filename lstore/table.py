from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.base_page = [[Page()] for _ in range(num_columns)]
        self.tail_pages = []
        self.rid_counter = 1
        pass
    def insert_row(self, columns):
        rid = self.table[columns]
        for i, value in enumerate(columns):
            current_page = self.base_page[i][-1]
            if not current_page.has_capacity:
                new_page = Page()
                self.base_page[i].append(new_page)
                current_page = new_page
            current_page.write(column)
        
        self.rid_counter += 1
        

    def __merge(self):
        print("merge is happening")
        pass
 
