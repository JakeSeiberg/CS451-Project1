from lstore.table import Table, Record
from lstore.page import Page
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            rid = self.table.index.locate(self.table.key, primary_key)
            if rid is None or rid not in self.table.page_directory:
                return False
            del self.table.page_directory[rid]
            return True
        except:
            return False
    
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        primary_key = columns[self.table.key]
        existing_rid = self.table.index.locate(self.table.key, primary_key)

        if existing_rid is not None:
            return False
        
        rid = self.table.insert_row(list(columns))
        
        if rid is not None:
            return True
        else:
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        rid = self.table.index.locate(search_key_index, search_key)
        if rid is None:
            return []
        locations = self.table.page_directory[rid]
        record_values = []
        for col_idx, (page_idx, slot_idx) in enumerate(locations):
            if projected_columns_index[col_idx]:
                value = self.table.read_column(col_idx, page_idx, slot_idx)
                record_values.append(value)
            else:
                record_values.append(None)
        return [Record(rid, search_key, record_values)]


    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rid = self.table.index.locate(search_key_index, search_key)
        if (rid is None or rid not in self.table.page_directory):
            return None
        
        record_values = []
        actual_version = relative_version

        if (relative_version < 0):
            if (rid not in self.table.version_chain or len(self.table.version_chain[rid]) == 0):
                actual_version = 0
            else:
                version_idx = -relative_version - 1
                if (version_idx >= len(self.table.version_chain[rid])):
                    version_idx = len(self.table.version_chain[rid]) - 1
                    actual_version = -(version_idx + 1)
    
        for col_idx, is_projected in enumerate(projected_columns_index):
            if (is_projected == 1):
                if (actual_version == 0):
                    page_index, record_offset = self.table.page_directory[rid][col_idx]
                    value = self.table.read_column(col_idx, page_index, record_offset)
                    record_values.append(value)
                else:
                    version_idx = -actual_version - 1
                    tail_locations = self.table.version_chain[rid][version_idx]
                    
                    if (tail_locations[col_idx] is not None):
                        page_index, record_offset = tail_locations[col_idx]
                        page = self.table.tail_page[col_idx][page_index]
                        value = page.read(record_offset)
                        record_values.append(value)
                    else:
                        value = None
                        for older_idx in range(version_idx + 1, len(self.table.version_chain[rid])):
                            older_tail = self.table.version_chain[rid][older_idx]
                            if (older_tail[col_idx] is not None):
                                page_index, record_offset = older_tail[col_idx]
                                page = self.table.tail_page[col_idx][page_index]
                                value = page.read(record_offset)
                                break
                        if (value is None):
                            page_index, record_offset = self.table.page_directory[rid][col_idx]
                            value = self.table.read_column(col_idx, page_index, record_offset)
                        
                        record_values.append(value)
            else:
                record_values.append(None)
        
        return [Record(rid, search_key, record_values)]

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        rid = self.table.index.locate(self.table.key, primary_key)
        if rid is None or rid not in self.table.page_directory:
            return False
        
        if columns[self.table.key] is not None:
            new_primary_key = columns[self.table.key]
            if new_primary_key != primary_key:  # Only check if actually changing the key
                existing_rid = self.table.index.locate(self.table.key, new_primary_key)
                if existing_rid is not None:
                    return False
        
        old_locations = self.table.page_directory[rid]
        tail_locations = [None] * self.table.num_columns
        for col_idx, new_value in enumerate(columns):
            if new_value is not None:
                page_idx, slot_idx = old_locations[col_idx]
                old_value = self.table.read_column(col_idx, page_idx, slot_idx)
                tail_page = self.table.tail_page[col_idx][-1]
                if not tail_page.has_capacity():
                    new_tail = Page()
                    self.table.tail_page[col_idx].append(new_tail)
                    tail_page = new_tail
                tail_page.write(old_value)
                tail_page_idx = len(self.table.tail_page[col_idx]) - 1
                tail_slot_idx = tail_page.num_records - 1
                tail_locations[col_idx] = (tail_page_idx, tail_slot_idx)
                offset = slot_idx * 8
                page = self.table.base_page[col_idx][page_idx]
                page.data[offset:offset + 8] = new_value.to_bytes(8, byteorder='little', signed=True)
        if rid not in self.table.version_chain:
            self.table.version_chain[rid] = []
        self.table.version_chain[rid].insert(0, tail_locations) 
        
        return True

    


    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        output = 0
        found = False
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue
            value = None
            if relative_version == 0:
                page_index, record_offset = self.table.page_directory[rid][aggregate_column_index]
                value = self.table.read_column(aggregate_column_index, page_index, record_offset)
            else:
                version_idx = -relative_version - 1
                if rid in self.table.version_chain and version_idx < len(self.table.version_chain[rid]):
                    tail_locations = self.table.version_chain[rid][version_idx]
                    if tail_locations[aggregate_column_index] is not None:
                        page_index, record_offset = tail_locations[aggregate_column_index]
                        page = self.table.tail_page[aggregate_column_index][page_index]
                        value = page.read(record_offset)
                    else:
                        value_found = False
                        for older_idx in range(version_idx + 1, len(self.table.version_chain[rid])):
                            older_tail = self.table.version_chain[rid][older_idx]
                            if older_tail[aggregate_column_index] is not None:
                                page_index, record_offset = older_tail[aggregate_column_index]
                                page = self.table.tail_page[aggregate_column_index][page_index]
                                value = page.read(record_offset)
                                value_found = True
                                break
                        if not value_found:
                            page_index, record_offset = self.table.page_directory[rid][aggregate_column_index]
                            value = self.table.read_column(aggregate_column_index, page_index, record_offset)
                else:
                    if rid in self.table.version_chain and len(self.table.version_chain[rid]) > 0:
                        oldest_tail = self.table.version_chain[rid][-1]
                        if oldest_tail[aggregate_column_index] is not None:
                            page_index, record_offset = oldest_tail[aggregate_column_index]
                            page = self.table.tail_page[aggregate_column_index][page_index]
                            value = page.read(record_offset)
                        else:
                            page_index, record_offset = self.table.page_directory[rid][aggregate_column_index]
                            value = self.table.read_column(aggregate_column_index, page_index, record_offset)
                    else:
                        page_index, record_offset = self.table.page_directory[rid][aggregate_column_index]
                        value = self.table.read_column(aggregate_column_index, page_index, record_offset)
            if value is not None:
                output += value
                found = True
        
        return output if found else False



    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    
    def sum(self, start_range, end_range, aggregate_column_index):
        output = 0
        found = False
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        for rid in rid_list:
            if rid in self.table.page_directory:
                page_index, record_offset = self.table.page_directory[rid][aggregate_column_index]
                value = self.table.read_column(aggregate_column_index, page_index, record_offset)
                output += value
                found = True

        if not found:
            return False
        
        return output
