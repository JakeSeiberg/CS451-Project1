
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self, num):
        if num >= 512:
            return False
        else:
            return True


    def write(self, value):
        if self.num_records <= 512:
            self.data.append(value)
            self.num_records += 1:
        else:
            has_capacity()

