import pandas
import MelbourneHousingMarketClass as MHMClass
from math import isnan

filename2 = ''


class Data:

    def __init__(self, filename):
        self.data = pandas.read_csv(filename, header=0)
        self.header_dict = {}
        self.house_array = []

    def create_header_dict(self):
        temp = self.get_header()
        for c in range(len(temp)):
            self.header_dict[c] = temp[c]

        self.header_dict[0] = "id"

    def get_header(self, col=None):
        if col is None:
            return self.data.columns
        else:
            return self.data.columns[col]

    def create_house_array_from_dataset(self):
        for id in range(17213, self.data.shape[0]):
            self.house_array.append(MHMClass.MHMC(
                self.header_dict,
                id,
                self.data.iloc[id, 1],
                self.data.iloc[id, 2],
                self.data.iloc[id, 3],
                self.data.iloc[id, 4],
                self.data.iloc[id, 5],
                self.data.iloc[id, 6],
                self.data.iloc[id, 7],
                self.data.iloc[id, 8],
                self.data.iloc[id, 9],
                self.data.iloc[id, 10],
                self.data.iloc[id, 11],
                self.data.iloc[id, 12],
                self.data.iloc[id, 13],
                self.data.iloc[id, 14],
                self.data.iloc[id, 15],
                self.data.iloc[id, 16],
                self.data.iloc[id, 17],
                self.data.iloc[id, 18],
                self.data.iloc[id, 19],
                self.data.iloc[id, 20],
                self.data.iloc[id, 21]
            ))


class DataCleaner(Data):

    def clean_all(self):
        for c in range(self.data.shape[1]):
            self.clean_empty_box(c)

    def clean_empty_box(self, col=None):
        for _ in range(self.data.shape[0]):
            if type(self.data.iloc[_, col]) is str:
                pass
            elif isnan(self.data.iloc[_, col]):
                self.data.iloc[_, col] = -1

    def write_to_csv(self):
        pandas.DataFrame.to_csv(self.data, filename2)












