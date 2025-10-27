
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self, num):
        return self.num_records < 512

    def write(self, value):
        if self.has_capacity(self.num_records):
            offset = self.num_records
            self.data[offset] = value
            self.num_records += 1
            return True
        else:
            return False

