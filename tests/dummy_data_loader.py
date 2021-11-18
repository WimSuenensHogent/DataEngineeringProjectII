import pandas as pd


class DummyDataLoader:

    def __init__(self):
        self.prefix = "dummydata"

    def load(self, data):
        df = pd.read_csv(f"{self.prefix}/{data}", sep=',')
        return df




