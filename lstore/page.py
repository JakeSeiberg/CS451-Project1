
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self, num):
        return self.num_records < 512

    def write(self, value):
        if self.has_capacity(self.num_records):
            offset = self.num_records * 8
            self.data[offset:offset + 8] = value.to_bytes(8, byteorder='little', signed=True)
            self.num_records += 1
            return True
        else:
            return False
    def read(self, slot):
        offset = 8 * slot
        data_bytes = self.data[offset:offset+8]
        return int.from_bytes(data_bytes,byteorder='little', signed=True)

