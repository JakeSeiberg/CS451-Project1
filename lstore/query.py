from lstore.table import Table, Record
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
    """ #this one
    def delete(self, primary_key):
        
        if True:
            self.table.page_directory[primary_key]
            return True
        else:
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """ #this one
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
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
    """ #this one
    def select(self, search_key, search_key_index, projected_columns_index):
        # Locate the RID using the index
        rid = self.table.index.locate(search_key_index, search_key)
        if rid is None:
            return []  # No matching record

        # Retrieve the page positions for that RID
        record_positions = self.table.page_directory[rid]
        
        # Read column values
        record_values = []
        for col_idx, (page_idx, slot_idx) in enumerate(record_positions):
            if projected_columns_index[col_idx]:
                value = self.table.read_column(col_idx, page_idx, slot_idx)
                record_values.append(value)
            else:
                record_values.append(None)  # Optional: return None for non-projected columns

        # Return as a list with a single Record
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
    """ #maybe this one
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """ #this one
    def update(self, primary_key, *columns):
    # Find the RID for this primary key
        rid = self.table.index.locate(self.table.key, primary_key)
        if rid is None or rid not in self.table.page_directory:
            return False
        
        # Get the record positions
        record_positions = self.table.page_directory[rid]
        
        # Update each column that isn't None
        for col_idx, new_value in enumerate(columns):
            if new_value is not None:
                page_idx, slot_idx = record_positions[col_idx]
                page = self.table.base_page[col_idx][page_idx]
                # Write the new value to the page
                offset = slot_idx * 8
                page.data[offset:offset + 8] = new_value.to_bytes(8, byteorder='little', signed=True)
        
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
        pass

    
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
    """ #fix return False error handling (if empty record)
    
    def sum(self, start_range, end_range, aggregate_column_index):
        output = 0
        found = False
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        #print(f"DEBUG: Range [{start_range}, {end_range}], col={aggregate_column_index}, RIDs found: {rid_list}")
        
        for rid in rid_list:
            if rid in self.table.page_directory:
                # Get the primary key for this RID
                pk_page_index, pk_record_offset = self.table.page_directory[rid][self.table.key]
                pk_value = self.table.read_column(self.table.key, pk_page_index, pk_record_offset)
                
                page_index, record_offset = self.table.page_directory[rid][aggregate_column_index]
                value = self.table.read_column(aggregate_column_index, page_index, record_offset)
                #print(f"  RID {rid}: primary_key={pk_value}, column[{aggregate_column_index}]={value}")
                output += value
                found = True

        #print(f"  Total sum: {output}")
        if not found:
            return False
        
        return output
