import pyrebase
import DataSetHandler as DSH
import numpy as np
from NoSQLKey import config

cols = {'id': 0, 'suburb': 1, 'adress': 2, 'rooms': 3, 'type': 4, 'price': 5, 'method': 6, 'SellerG': 7, 'Date': 8, 'Distance': 9, 'postcode': 10,
        'bed': 11, 'bath': 12, 'car': 13, 'landsize': 14, 'buildingarea': 15, 'yearbuilt': 16, 'councilarea':17, 'latitude': 18, 'longitude':19,
        'regionname': 20, 'propertycount': 21}


class DataBase:

    def __init__(self, user, password):
        self.fb = pyrebase.initialize_app(config)
        self.connect(user, password)

    def connect(self, user, password):
        self.auth = self.fb.auth()
        self.user = self.auth.sign_in_with_email_and_password(user, password)

    def database_queries(self, query=''):

        def get_suberb():

            temp = self.fb.database().child('suberb').child('Abbotsford').get(self.user['idToken'])

            for id in str(temp.val().values().__iter__().__next__()).split(","):
                print(int(id))

        queries = {'get_suberb': get_suberb}

        return queries[query]

    def database_ops(self, data=None, op=''):


        def divide_prize(d):

            prices = [data.iloc[x, cols['price']] for x in range(data.shape[0]) if data.iloc[x, cols['price']] > -1]
            prices_set_sorted = sorted(set(prices))

            q1 = prices_set_sorted[int(len(prices_set_sorted)/4)]
            q2 = prices_set_sorted[int(len(prices_set_sorted)/2)]
            q3 = prices_set_sorted[int(len(prices_set_sorted) / 2 + int(len(prices_set_sorted)/4))]

            new_d = {-1: "", q1: "", q2: "", q3: "", q3+1: ""}

            for price, ids in d.items():
                if price == -1:
                    new_d[-1] += ids
                elif price <= q1:
                    new_d[q1] += ids
                elif price > q1 and price <= q2:
                    new_d[q2] += ids

                elif price > q2 and price <= q3:
                    new_d[q3] += ids
                else:
                    new_d[q3+1] += ids

            return int(q1), int(q2), int(q3), new_d

        def make_dict_adv(col, f=None):
            d = {}

            for id in range(data.shape[0]):
                if f(data.iloc[id, col]) not in d:
                    d[f(data.iloc[id, col])] = ""

                if d[f(data.iloc[id, col])] == "":
                    d[f(data.iloc[id, col])] += str(id)
                else:
                    d[f(data.iloc[id, col])] += "," + str(id)

            return d

        def make_dict(col):
            d = {}

            for id in range(data.shape[0]):
                if data.iloc[id, col] not in d:
                    d[data.iloc[id, col]] = ""

                if d[data.iloc[id, col]] == "":
                    d[data.iloc[id, col]] += str(id)
                else:
                    d[data.iloc[id, col]] += "," + str(id)

            return d

        def create_suberb():
            d = make_dict(cols['suburb'])

            for suburb, ids in d.items():
                self.fb.database().child('regionname')\
                                  .child(data.iloc[int(str.split(ids, ",")[0]), cols['regionname']])\
                                  .child(suburb)\
                                  .push(ids, self.user['idToken'])

        def create_type():
            d = make_dict(cols['type'])

            for typ, ids in d.items():
                self.fb.database().child('type').child(typ).push(ids, self.user['idToken'])

        def create_rooms(room_type):
            d = make_dict(cols[room_type])

            for room, ids in d.items():
                self.fb.database().child('price')\
                                  .child(room_type)\
                                  .child(str(int(room)))\
                                  .push(ids, self.user['idToken'])

        def create_price():

            d = make_dict(cols['price'])
            q1, q2, q3, d = divide_prize(d)

            for price, ids in d.items():

                if price == -1:
                    self.fb.database().child('price').child("-1").push(ids, self.user['idToken'])
                elif price <= q1:
                    self.fb.database().child('price').child("0-"+str(q1)).push(ids, self.user['idToken'])
                elif price > q1 and price <= q2:
                    self.fb.database().child('price').child(str(q1)+"-"+str(q2)).push(ids, self.user['idToken'])
                elif price > q2 and price <= q3:
                    self.fb.database().child('price').child(str(q2) + "-" + str(q3)).push(ids, self.user['idToken'])
                else:
                    self.fb.database().child('price').child(str(q3)+"+").push(ids, self.user['idToken'])

        def create_year_sold():

            d_year = make_dict_adv(cols['Date'], (lambda x: str.split(x, '/')[2]))
            d_month = make_dict_adv(cols['Date'], (lambda x: str.split(x, '/')[1]))

            for year, ids_y in d_year.items():
                for month, ids_m in d_month.items():
                    temp = np.intersect1d(str.split(ids_y, ","), str.split(ids_m, ","))
                    self.fb.database().child('year').child(str(year)).child(str(month)).push(dict(temp), self.user['idToken'])

        def create_land_size():
            pass

        def create_building_area():
            pass

        def create_seller():
            pass

        def create_full_houses():
            if data is None:
                error('d')

            for house in data:
                self.fb.database().child("houses").child(house.json['id']).push(house.json, self.user['idToken'])

        def get_num_of_houses():
            houses = self.fb.database().child('houses').get(self.user['idToken'])
            print(len(houses.val())-1)

        def error(e):
            if e == '':
                raise "Error - must choose a function\n'c' for create all houses\n'n' to get number of houses in db"
            if e == 'd':
                raise "Error - must provide data as type MelbourneHousingMarketClass"
            if e == 'f':
                raise 'Invalid function type'

        ops = {'': error,
               'create': create_full_houses,
               'create_type': create_type,
               'create_room': create_rooms,
               'create_price': create_price,
               'create_year': create_year_sold,
               'num_of_houses': get_num_of_houses,
               'cs': create_suberb}

        if op not in ops:
            error('f')

        return ops[op]
