from lstore.index import Index
from time import time
from lstore.page import Page

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
        self.tail_page = [[Page()] for _ in range(num_columns)]
        self.rid_counter = 0
        self.version_chain = {}

        self.index.create_index(self.key)

    def insert_row(self, columns):
        rid = self.rid_counter + 1
        self.rid_counter += 1
        page_positions = [None] * self.num_columns
   
        for i, value in enumerate(columns):
            current_page = self.base_page[i][-1]
            if not current_page.has_capacity():
                new_page = Page()
                self.base_page[i].append(new_page)
                current_page = new_page
            current_page.write(value)
            page_index = len(self.base_page[i])-1
            record_offset = self.base_page[i][-1].num_records - 1
            page_positions[i] = (page_index, record_offset)
            
        self.page_directory[rid] = page_positions
        primary_key_value = columns[self.key]
        self.index.insert(self.key, primary_key_value, rid)
        return rid
        
    def read_column(self,col_idx, page_idx, slot_idx):
        page = self.base_page[col_idx] [page_idx]
        return page.read(slot_idx)
        

    def __merge(self):
        print("merge is happening")
        pass
 