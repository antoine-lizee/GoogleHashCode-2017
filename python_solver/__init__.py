from pprint import pprint


class Object:
    def __init__(self, file_path):
        with open(file_path) as f:
            self.data = f.readlines()

    def __repr__(self):
        pprint(self.data)


# Test