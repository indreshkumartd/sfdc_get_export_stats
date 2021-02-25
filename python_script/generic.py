import pytd
import json
import pandas as pd

def connect_td_db(apikey, endpoint, dbname, default_engine='presto'):
    """
    Attempt to connect TD
    :param apikey:
    :param endpoint:
    :param dbname:
    :param default_engine:
    :return:
    """
    try:
        cli = pytd.Client(apikey=apikey,
                          endpoint=endpoint,
                          database=dbname,
                          default_engine=default_engine)
        print('Connection Successful')
        return cli
    except Exception as e:
        print('Exception in connect_td_db(): ', str(e))
        cli.close()

def load_td_table(tab_df_list, if_exists='append'):
    """
    Load df to TD Table
    :param tab_df_list - contains table, dataframe, TD_Connection
    :param if_exists:
    :return: None if successful
    """
    try:
        dest_table, dataframe, client = tab_df_list
        if dataframe.empty:
            print(f'Table {dest_table} has no new data to load...')
        else:
            # Converting 'NaN' to NULL
            dataframe = dataframe.where(pd.notnull(dataframe), '')

            dest_table = dest_table.lower()
            client.load_table_from_dataframe(dataframe, dest_table, if_exists=if_exists)
            print('Rows: ', str(len(dataframe)), ' are ', if_exists, ' in ', dest_table, ' successfully...')
        return None
    except Exception as e:
        print('Exception in load_td_table_new(): ', str(e))
        raise
