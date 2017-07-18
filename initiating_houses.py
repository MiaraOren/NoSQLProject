import DataSetHandler as DSH
import DataBaseHandler as DBH
from sys import argv

filename = "Melbourne_housing_extra_data_cleaned.csv"

user = "miaraoren@gmail.com"
password = "test123"


def main(args):

    """

        Initiate Connecting to Fire base and restart dataset
        DBH.DataBase -> connects to firebase with authentication
        DSH.DataCleaner -> reads csv from clean file ( all 'nan' variables are -1 )

    """

    # Connecting Fire-Base
    data_base = DBH.DataBase(user, password)

    # Reading the Cleaned data
    data_set = DSH.DataCleaner(filename)

    # Creating dictionary of columns from Header
    data_set.create_header_dict()

    # Creating Array of Houses ( array of class
    data_set.create_house_array_from_dataset()

    '''
        Operations By User
    '''

    ops = {'create': data_base.database_ops(data=data_set.house_array, op='create'),
           'create_sub': data_base.database_ops(data=data_set.data, op='cs'),
           'create_region': data_base.database_ops(data=data_set.data, op='cr')
           }

    '''
        Queries By User
    '''

    queries = {'get_suberb': data_base.database_queries('get_suberb')}

    '''
        Actions By User
    '''

    ops[args[0]]()

if __name__ == '__main__':
    main(argv[1:])