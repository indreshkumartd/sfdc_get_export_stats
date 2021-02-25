# -*- coding: utf-8 -*-

def main():
    import os
    import sys
    import requests
    from datetime import datetime, timedelta

    ############################ Bootstrap START #################################
    os.system(f"{sys.executable} -m pip install pytd")
    os.system(f"{sys.executable} -m pip install pandas")
    ############################# Bootstrap END ################################

    ############################# import Libs START ################################
    import pandas as pd
    from pandas.io.json import json_normalize
    from python_script.generic import connect_td_db
    from python_script.generic import load_td_table
    ############################# import Libs END ################################

    # TD and SFDC Details
    dest_tbl = os.getenv('dest_tbl')
    dest_db = os.getenv('dest_db')
    if_exists = os.getenv('if_exists')
    apikey = os.getenv('TD_API_KEY')
    endpoint = os.getenv('endpoint')
    object_name = os.getenv('object_name')
    createdById = os.getenv('createdById')
    base_url = os.getenv('api_base_url')
    # SFDC Details
    client_id = os.getenv('client_id')
    client_secret = os.getenv('client_secret')
    username = os.getenv('username')
    password = os.getenv('password')


    print("Hello TD")
    def get_access_token():
        try:
            url = f"{base_url}/oauth2/token"
            payload = {'grant_type': 'password',
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'password': password}
            files = [
            ]

            headers = {
            "Content-Type": "application/x-www-form-urlencoded"
            }

            response = requests.request("POST", url, headers=headers, data=payload, files=files)
            # print(response.text.encode('utf8'))
            # print(response.text)
            if response.status_code == 200:
                _access_token = response.json().get('access_token')
                print(_access_token)
                return _access_token
            print('Something went wrong: response.status_code = ', response.status_code)
            return None
        except Exception as e:
            print('Something went wrong. please check. ', str(e))
            raise


    #get the list of td_jobid for latest push:
    def get_td_td_jobid(object_name, createdById, _access_token):
        # td_jobid = '750P0000004xlkaIAA'
        try:
            today_date = (datetime.now()).strftime('%Y-%m-%d')
            print(f'Getting the td_jobid for object: {object_name}, createdById: {createdById}, Date: {today_date}')
            url = f"{base_url}/data/v51.0/jobs/ingest/"
            payload = {}
            headers = {
              'Authorization': f'Bearer {password}{_access_token}'
            }
            response = requests.request("GET", url, headers=headers, data=payload)
            if response.status_code == 200:
                res = response.json()
                res_list = res.get("records")
                # res_list.sort(key=lambda x: datetime.strptime(x['createdDate'], '%Y-%m-%dT%H:%M:%S.000+0000'),
                #               reverse=True)  # 2021-02-14T23:08:03.000+0000",
                td_jobid_list = []
                for item in res_list:
                    # print(item)
                    for key, val in item.items():
                        # print(key, val)
                        if item.get("createdById") == createdById and today_date in item.get("createdDate") and item.get(
                                "object") == object_name: #and item.get('id') == '750P0000004xlkaIAA'
                            # print(item)
                            td_jobid = item.get('id')
                            # print(td_jobid, item.get("createdDate"))
                            # return td_jobid
                            td_jobid_list.append(td_jobid)
                        else:
                            pass
                # Get unique list of job ids
                td_jobid_list = list(set(td_jobid_list))
                # print(td_jobid)
                # print(len(td_jobid))
                return td_jobid_list
            else:
                print('Something went wrong: response.status_code = ', response.status_code)
                return None
        except Exception as e:
            print('Something went wrong. please check. ', str(e))
            raise

    # get the result of td_jobid
    def get_result_of_td_jobid(td_jobid, _access_token):
        if td_jobid:
            url = f"{base_url}/data/v51.0/jobs/query/{td_jobid}/"
            payload = {}
            headers = {
              'Authorization': f'Bearer {password}{_access_token}'
            }
            try:
                response = requests.request("GET", url, headers=headers, data=payload)
                # print(response.text.encode('utf8'))
                if response.status_code == 200:
                    res = response.json()
                    # print(res)
                    return res
                return None
            except Exception as e:
                print('Something went wrong. please check. ', str(e))
                raise

    #############################################################
    ##### CODE STARTS HERE #####
    #############################################################
    try:
        final_df = pd.DataFrame()
        # get the Access Token
        _access_token = get_access_token()
        if _access_token:
            td_jobid = get_td_td_jobid(object_name, createdById, _access_token)
            print(f'td_jobid for object {object_name} is: {td_jobid}')
            if td_jobid:
                for jobid in td_jobid:
                    res_dict = get_result_of_td_jobid(jobid, _access_token)
                    df = pd.DataFrame.from_dict(json_normalize(res_dict), orient='columns')
                    # print(df.to_string())
                    final_df = final_df.append(df, sort=False)

                # Calling function to connect to TD DB
                client = connect_td_db(apikey, endpoint, dest_db)
                # call function to load the data to td
                load_td_table((dest_db + '.' + dest_tbl, final_df, client), if_exists)
            else:
                print(f'There is no JobId for given date for createdById: {createdById}. Please check api response from get_td_td_jobid() method')
        else:
            print("Could not get the get access_token. Please check api response from get_access_token() method")
    except Exception as e:
        print('Something went wrong here. please check. ', str(e))
        raise
    finally:
        try:
            client.close()
        except:
            pass
        print('TD Connection Closed.')

    #############################################################
    ##### CODE ENDS HERE #####
    #############################################################
