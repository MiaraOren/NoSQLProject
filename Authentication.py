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

    '''
        Operations By User
    '''
    '''

    ops = {'create': data_base.database_ops(data=data_set.house_array, op='create'),
           'create_sub': data_base.database_ops(data=data_set.data, op='cs'),
           'create_type': data_base.database_ops(data=data_set.data, op='create_type'),
           'create_room': data_base.database_ops(data=data_set.data, op='create_room'),
           'create_price': data_base.database_ops(data=data_set.data, op='create_price'),
           'create_year': data_base.database_ops(data=data_set.data, op='create_year'),
           'num_of_houses': data_base.database_ops(op='num_of_houses')
           }
    '''
    '''
        Queries By User
    '''
    queries = {'get_by_suberb': data_base.database_queries('get_by_suberb'),
               'get_by_price': data_base.database_queries('get_by_price')}



    def print_main_menu():
        choice = 0
        while (choice != 6):
            choice = input(
                'What would you like to do? \n '
                '0 - Find house with multiple filters\n'
                '1 - Find house by price\n '
                '2 - Find house by region \n '
                '3 - Find house by number of rooms \n '
                '4 - Find house by year \n '
                '5 - Find house by type \n '
                '6 - Find all houses \n'
                '7 - Exit \n')
            v = 'ten'
            if (choice == '0'):
                pass
            elif (choice == '1'):
                find_house_by_price();
            elif (choice == '2'):
                find_house_by_suberb();
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
        pass

    def find_house_by_suberb(self=None):
        pass

    def find_house_by_type(self=None):
        pass

    def find_house_by_year(self=None):
        pass

    def find_all_houses(self=None):
        pass



    print_main_menu()

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

    #ops['create_room']('bed')
    #ops['create_room']('bath')
    #ops['create_year']()

    # '''
    # Call select query with params
    # '''
    # queries['get_by_price'](1,100)




if __name__ == '__main__':
    main(argv[1:])