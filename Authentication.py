import DataSetHandler as DSH
import DataBaseHandler as DBH
from time import time
from sys import argv

filename = "Melbourne_housing_extra_data_cleaned.csv"

user = "miaraoren@gmail.com"
password = "test123"



def main(args):

    """
        Initiate Connecting to Fire base and restart dataset
    """

    # Connecting Fire-Base
    data_base = DBH.DataBase(user, password)

    # Reading the Cleaned data
    data_set = DSH.DataCleaner(filename)

    # Creating dictionary of columns from Header
    data_set.create_header_dict()

    # Creating Array of Houses ( array of class Melbourne... )
    #data_set.create_house_array_from_dataset()

    '''
        Operations By User
    '''

    ops = {'create': data_base.database_ops(data=data_set.house_array, op='create'),
           'create_sub': data_base.database_ops(data=data_set.data, op='cs'),
           'create_type': data_base.database_ops(data=data_set.data, op='create_type'),
           'create_room': data_base.database_ops(data=data_set.data, op='create_room'),
           'create_price': data_base.database_ops(data=data_set.data, op='create_price'),
           'create_year': data_base.database_ops(data=data_set.data, op='create_year'),
           'create_landsize': data_base.database_ops(data=data_set.data, op='create_landsize'),
           'create_buildingarea': data_base.database_ops(data=data_set.data, op='create_buildingarea'),
           'create_seller': data_base.database_ops(data=data_set.data, op='create_seller'),
           'num_of_houses': data_base.database_ops(op='num_of_houses')
           }

    '''
        Queries By User
    '''
    queries = {'get_by_suberb': data_base.database_queries('get_by_suberb'),
               'get_by_price': data_base.database_queries('get_by_price'),
               'get_by_year': data_base.database_queries('get_by_year'),
               'get_by_rooms': data_base.database_queries('get_by_rooms'),
               'get_by_type': data_base.database_queries('get_by_type'),
               'get_by_seller': data_base.database_queries('get_by_seller'),
               'get_all_houses': data_base.database_queries('get_all_houses')}



    def print_main_menu():
        choice = 0
        while (choice != 6):
            choice = input(
                'What would you like to do? \n '
                '0 - Find house with multiple filters\n'
                '1 - Find house by price\n '
                '2 - Find house by seller \n '
                '3 - Find house by number of rooms \n '
                '4 - Find house by year \n '
                '5 - Find house by type \n '
                '6 - Find all houses \n'
                '7 - Exit \n')
            v = 'ten'
            if (choice == '0'):
                print('not implemented')
            elif (choice == '1'):
                find_house_by_price();
            elif (choice == '2'):
                find_house_by_seller();
            elif (choice == '3'):
                find_house_by_rooms();
            elif (choice == '4'):
                find_house_by_year();
            elif (choice == '5'):
                find_house_by_type();
            elif (choice == '6'):
                find_all_houses();
            elif (choice == '7'):
                print('Exit program. Bye \n')
                quit()
            else:
                print('Invalid input, please try again \n')

    def find_house_by_price(self=None):
        input_required = True;
        while (input_required):
            price = input('Enter desired price ')
            sign = input('Should price be less (L), more (M), or nevermind (N)?')
            if (sign == 'L' or sign == 'l'):
                # 1 more than, 0 NM, -1 less than
                queries['get_by_price'](-1, price)
                input_required = False
            elif (sign == 'M' or sign == 'm'):
                queries['get_by_price'](1, price)
            elif (sign == 'N' or sign == 'n'):
                queries['get_by_price'](0, price)
            else:
                print('Invalid input, please try again \n')

    def find_house_by_rooms(self=None):
        input_required = True;
        room_type = 'bed'
        while (input_required):
            roomType = input('Enter desired room type [1 - bathroom, 2 - bedroom] ')

            if (roomType == '1' or roomType == '2'):
                number_of_rooms = input('Enter desired number of rooms ')
                if (roomType == '1'):
                    room_type = 'bath'
                queries['get_by_rooms'](room_type, number_of_rooms)
                input_required = False
            else:
                print('Invalid input, please try again \n')

    def find_house_by_suberb(self=None):
        pass

    def find_house_by_seller(self=None):
        input_required = True;
        while (input_required):
            seller = input('Choose one of the sellers 1 - Barry, 2 - Greg, 3 - Nelson, 4 - Ray ')

            if (seller == '1' or seller == '2' or seller == '3' or seller == '4'):
                queries['get_by_seller'](seller)
                input_required = False
            else:
                print('Invalid input, please try again \n')

    def find_house_by_type(self=None):
        input_required = True;
        while (input_required):
            type = input('Enter desired property type [ h or t or u ] ')

            if (type == 'h' or type == 't'  or type == 'u'):
                queries['get_by_type'](type)
                input_required = False
            else:
                print('Invalid input, please try again \n')

    def find_house_by_year(self=None):
        input_required = True;
        while (input_required):
            year = input('Enter desired year (2017/2016) ')
            if year == '2017' or year == '2016':

                month = input('Enter month 01-12 ')
                if month == '01' or month == '02' or month == '03' or month == '04' or month == '05' or month == '06' or month == '07' or month == '08' or month == '09' or month == '10' or month == '11' or month == '12':
                    queries['get_by_year'](year, month)
                    input_required = False
                else:
                    print('Invalid input, please try again \n')
            else:
                print('Invalid input, please try again \n')

    def find_all_houses(self=None):
        queries['get_all_houses']()


    '''
        Main Menu Functions
    '''
    print_main_menu()
    #data_base.database_ops(data=data_set.data, op='test')()
    # Reading the Cleaned data
    #data_set = DSH.DataCleaner(filename)

    # Creating dictionary of columns from Header
    #data_set.create_header_dict()

    # Creating Array of Houses ( array of class
    #data_set.create_house_array_from_dataset()

    # '''
    #     Operations By User
    # '''
    # '''
    #
    # ops = {'create': data_base.database_ops(data=data_set.house_array, op='create'),
    #        'create_sub': data_base.database_ops(data=data_set.data, op='cs'),
    #        'create_type': data_base.database_ops(data=data_set.data, op='create_type'),
    #        'create_room': data_base.database_ops(data=data_set.data, op='create_room'),
    #        'create_price': data_base.database_ops(data=data_set.data, op='create_price'),
    #        'create_year': data_base.database_ops(data=data_set.data, op='create_year'),
    #        'num_of_houses': data_base.database_ops(op='num_of_houses')
    #        }
    # '''
    # '''
    #     Queries By User
    # '''
    # queries = {'get_by_suberb': data_base.database_queries('get_by_suberb'),
    #            'get_by_price': data_base.database_queries('get_by_price')}
    #
    '''
        Actions By User
    '''

    #ops['create_seller']()

    # '''
    # Call select query with params
    # '''
    # queries['get_by_price'](1,100)



if __name__ == '__main__':
    main(argv[1:])