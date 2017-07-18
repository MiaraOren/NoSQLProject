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
    #data_base = DBH.DataBase(user, password)

    # Reading the Cleaned data
    #data_set = DSH.DataCleaner(filename)

    # Creating dictionary of columns from Header
    #data_set.create_header_dict()

    # Creating Array of Houses ( array of class
    #data_set.create_house_array_from_dataset()

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
    #queries = {'get_suberb': data_base.database_queries('get_suberb')}

    '''
        Actions By User
    '''

    #ops['create_room']('bed')
    #ops['create_room']('bath')
    #ops['create_year']()



if __name__ == '__main__':
    main(argv[1:])