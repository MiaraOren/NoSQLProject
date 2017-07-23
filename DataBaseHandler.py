import pyrebase
import DataSetHandler as DSH
import numpy as np
from time import time
from NoSQLKey import config
import matplotlib.pyplot as plt
import pandas as pd
cols = {'id': 0, 'suburb': 1, 'adress': 2, 'rooms': 3, 'type': 4, 'price': 5, 'method': 6, 'SellerG': 7, 'Date': 8, 'Distance': 9, 'postcode': 10,
        'bed': 11, 'bath': 12, 'car': 13, 'landsize': 14, 'buildingarea': 15, 'yearbuilt': 16, 'councilarea':17, 'latitude': 18, 'longitude':19,
        'regionname': 20, 'propertycount': 21}


price_benchmark_10 = []
price_benchmark_100 = []
price_benchmark_1000 = []
price_benchmark_1 = []


class DataBase:

    def __init__(self, user, password):
        self.fb = pyrebase.initialize_app(config)
        self.connect(user, password)

    def connect(self, user, password):
        self.auth = self.fb.auth()
        self.user = self.auth.sign_in_with_email_and_password(user, password)

    def database_queries(self, query=''):

        def get_by_suberb():

            temp = self.fb.database().child('zina').get(self.user['idToken'])

            for id in str(temp.val().values().__iter__().__next__()).split(","):
                print(int(id))

        def get_by_price(comparator, amount, batch=1):
            allPricesRange = [-1, 644000,1017000, 1682000]
            pricesDict = {-1: '-1',644000: '0-644000', 1017000: '644000-1017000', 1682000: '1017000-1682000', 1682001: '1682000'}
            allpricesStr = ['-1','0-644000', '644000-1017000', '1017000-1682000', '1682000']
            pricesListForQuery = []
            find_by = 'price '

            # price less than amount
            if comparator == -1:
                find_by+'less than '+str(amount)
                for p in pricesDict:
                # for p in int(allPricesRange):
                    if (int(p) < int(amount) and int(p) != -1):
                        pricesListForQuery.append(pricesDict.get(p))

            # price more than amount
            if comparator == 1:
                find_by + 'more than ' + str(amount)
                for p in pricesDict:
                # for p in int(allPricesRange):
                    if (int(p) > int(amount)):
                        pricesListForQuery.append(pricesDict.get(p))

            if comparator == 0:
                pricesListForQuery = allPricesRange


            listOfResults = []

            time0 = time()


            # house_ids_by_price = self.fb.database().child('price').get(self.user['idToken'])
            for price in pricesListForQuery:
                house_ids_by_price = self.fb.database().child('price').child(price).get(self.user['idToken'])
                listOfResults.append(house_ids_by_price)



            time1 = time()
            # print('time to get prices ' , time1-time0)
            house_ids = []
            i = 0
            if (len(listOfResults) > 0):
                for id in str(listOfResults.__getitem__(i).val().values().__iter__().__next__()).split(","):
                    #print(id)
                    i = i+1
                    house_ids.append(id)

            counter = 0
            for n in range(0, len(house_ids), batch):

                if counter > 100:
                    break

                t, houses = get_houses_by_id(house_ids[n:n+batch], time1-time0, find_by)
                price_benchmark_1000.append(t)
                counter += 1

                '''
                    keep = input("Show next batch ( Y/N ): ")
                    if keep.upper() != 'Y':
                        break
                '''

            plt.plot(price_benchmark_1000, 'bo')
            plt.plot(price_benchmark_1000, 'r--')
            plt.title('price_benchmark_1')
            plt.xlabel('iterations')
            plt.ylabel('query time (s)')
            plt.show()

        def get_by_type(type):
            find_by = 'type ' + type
            houses = []
            time0 = time()
            house_ids_by_type = self.fb.database().child('type').child(type).get(self.user['idToken'])
            time1 = time()
            print('time to get type ', time1 - time0)
            for id in str(house_ids_by_type.val().values().__iter__().__next__()).split(","):
                # print(id)
                houses.append(id)
            get_houses_by_id(houses, time1 - time0, find_by)
            # print_query_result(find_by, time1 - time0, houses, len(houses))

        def get_by_seller(seller):
            find_by = 'sellen name ' + seller
            houses = []
            time0 = time()
            house_ids_by_type = self.fb.database().child('SellersG').child(seller).get(self.user['idToken'])
            time1 = time()
            print('time to get type ', time1 - time0)
            for id in str(house_ids_by_type.val().values().__iter__().__next__()).split(","):
                # print(id)
                houses.append(id)
            get_houses_by_id(houses, time1-time0, find_by)
            # print_query_result(find_by, time1 - time0, houses, len(houses))

        def get_by_rooms(room_type, number_of_rooms):
            find_by = 'room type ' + str(room_type) + ' ,number of rooms ' + str(number_of_rooms)
            houses = []
            time0 = time()
            house_ids_by_rooms = self.fb.database().child('rooms').child(room_type).child(number_of_rooms).get(self.user['idToken'])
            time1 = time()
            print('time to get rooms ', time1 - time0)
            for id in str(house_ids_by_rooms.val().values().__iter__().__next__()).split(","):
                # print(id)
                houses.append(id)
            # get_houses_by_id(houses)
            get_houses_by_id(houses, time1 - time0, find_by)
            # print_query_result(find_by, time1-time0, houses, len(houses))

        def get_by_seller(seller_number):
            sellers = ['Barry','Greg','Nelson','Ray']
            seller = sellers.__getitem__(int(seller_number)-1)
            find_by = 'seller ' + str(seller)
            houses = []
            time0 = time()
            house_ids_by_seller = self.fb.database().child('SellerG').child(seller).get(self.user['idToken'])
            time1 = time()
            print('time to get by seller ', time1 - time0)

            for id in str(house_ids_by_seller.val().values().__iter__().__next__()).split(","):
                houses.append(id)
            print('found ' + str(len(houses)) + ' ids')
            get_houses_by_id(houses, time1 - time0, find_by)



        def print_query_result(find_by="", elapsed_time=0, results=[], number_of_found_houses=0):
            print('###############################################################')
            print('Find house by params ' + find_by)
            print('Found ' + str(number_of_found_houses) + ' results')
            print('Elapsed time ' + str(elapsed_time))
            print('###############################################################')

        def get_by_year(year, month):
            find_by = 'year '+ year + ' month ' + month
            houses = []
            time0 = time()
            house_ids_by_year = self.fb.database().child('year').child(year).child(month).get(self.user['idToken'])
            time1 = time()
            time3 = time1 - time0
            print('time to get rooms by year', time1 - time0)
            if house_ids_by_year.pyres is None:
                # print_query_result(find_by, time3)
                pass
            else:
                for id in str(house_ids_by_year.val().values().__iter__().__next__()).split(","):
                    houses.append(id)
                get_houses_by_id(houses, time1 - time0, find_by)
                # print_query_result(find_by, time3, houses, len(houses))

        def get_all_houses():
            find_by = 'get all houses'
            house = []
            houses = []

            time0 = time()

            houses_by_id = self.fb.database().child('houses').get(self.user['idToken'])

            for h in str(houses_by_id.val().__iter__().__next__()).split(","):
                    # print(h)
                house.append(h)

                # houses.append(houses_by_id.val.values())
            houses.append(house)
            # print("Houses size=" + str(houses.__len__()) + "  " + str(house))
            house = []
            time1 = time()
            # get_houses_by_id(houses, time1 - time0, find_by)
            print_query_result(find_by, time1-time0, houses, len(houses))
            # print('time to get all houses ', time1 - time0)
            # print('Found %d results', len(houses))

        def get_houses_by_id(ids, query_time, find_by):
            house = []
            houses = []

            time0 = time()
            # print('Number of found houses ', len(ids))
            for house_id in ids:
                houses_by_id = self.fb.database().child('houses').child(house_id).get(self.user['idToken'])
                if houses_by_id.pyres is not None:
                    for h in str(houses_by_id.val().values().__iter__().__next__()).split(","):
                        house.append(h)

                # houses.append(houses_by_id.val.values())
                    houses.append(house)
                    # print("Houses size=" + str(houses.__len__()) +"  " + str(house))
                    house = []
                else:
                    print('house is None')
            time1 = time()
            total_time = query_time + (time1 - time0)
            print_query_result(find_by, total_time, houses, len(houses))
            # print('time to get houses ', time1 - time0)
            # print('Found %d results', len(houses))
            #
            return total_time, houses



        queries = {'get_by_suberb': get_by_suberb,
                   'get_by_price' : get_by_price,
                   'get_by_year'  : get_by_year,
                   'get_by_rooms': get_by_rooms,
                   'get_by_type': get_by_type,
                   'get_by_seller': get_by_seller,
                   'get_all_houses':get_all_houses,
                   'print_query_result': print_query_result,
                   }

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

        def divide_by(col, n=4):

            d = make_dict(col)

            values = [data.iloc[x, col] for x in range(data.shape[0]) if data.iloc[x, col] > -1]
            n_values = sorted(set(values))

            l = [int(n_values[x]) for x in range(0, len(n_values), int(np.floor(len(n_values)/n)))]
            range_dict = {k: v for k, v in zip([str(l[x]) + '-' + str(l[x+1]) for x in range(len(l)-1)], np.repeat('', len(l)))}
            range_dict[str(l[-1])+'>'] = ''
            range_dict['-1'] = ''

            for size, ids in d.items():
                for range_ in range_dict.keys():
                    size = int(size)

                    if size == -1 or size == 0:
                        if range_dict['-1'] == '':
                            range_dict['-1'] += ids
                        else:
                            range_dict['-1'] += ',' + ids
                        break

                    if len(str.split(range_, '-')) == 1:
                        if range_dict[range_] == '':
                            range_dict[range_] += ids
                        else:
                            range_dict[range_] += ',' + ids
                        break

                    if int(size) < max(np.int32(str.split(range_, '-'))):
                        if range_dict[range_] == '':
                            range_dict[range_] += ids
                        else:
                            range_dict[range_] += ',' + ids
                        break

            return range_dict

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
                    temp = str.join(',', np.intersect1d(str.split(ids_y, ","), str.split(ids_m, ",")))
                    self.fb.database().child('year').child(str(year)).child(str(month)).push(temp, self.user['idToken'])

        def create_land_size(n):

            land_sizes = divide_by(cols['landsize'], n)
            for size, ids in land_sizes.items():
                self.fb.database().child('landsize').child(size).push(ids, self.user['idToken'])

        def create_building_area(n):

            building_area = divide_by(cols['buildingarea'], n)
            for size, ids in building_area.items():
                self.fb.database().child('buildingarea').child(size).push(ids, self.user['idToken'])

        def create_seller():

            d = make_dict(cols['SellerG'])
            for seller, ids in d.items():
                print(seller)
                #self.fb.database().child('SellerG').child(str(seller)).push(ids, self.user['idToken'])

        def create_full_houses():
            if data is None:
                error('d')

            for house in data:
                self.fb.database().child("houses").child(house.json['id']).push(house.json, self.user['idToken'])

        def get_num_of_houses():
            houses = self.fb.database().child('houses').get(self.user['idToken'])
            print(len(houses.val())-1)

        def test():

            d = {}
            l = []

            for id in range(data.shape[0]):
                for k, v in cols.items():
                    if v == 0:
                        d[k] = str(id)
                    else:
                        d[k] = str(data.iloc[id, v])

                l.append(d)
                d = {}

            t0 = time()
            self.fb.database().child("FULL_HOUSE").push(l, self.user['idToken'])
            t1 = time()

            print("Time ", t1-t0 )

        def error(e):
            if e == '':
                raise "Error - must choose a function\n'c' for create all houses\n'n' to get number of houses in db"
            if e == 'd':
                raise "Error - must provide data as type MelbourneHousingMarketClass"
            if e == 'f':
                raise 'Invalid function type'

        ops = {'': error,
               'test': test,
               'create': create_full_houses,
               'create_type': create_type,
               'create_room': create_rooms,
               'create_price': create_price,
               'create_year': create_year_sold,
               'create_landsize': create_land_size,
               'create_buildingarea': create_building_area,
               'num_of_houses': get_num_of_houses,
               'create_seller': create_seller,
               'cs': create_suberb}

        if op not in ops:
            error('f')

        return ops[op]
