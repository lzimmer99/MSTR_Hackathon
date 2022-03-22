import pandas as pd
import numpy
import json
import requests

from setup import *


class MSTR(object):
    def __init__(self, name):
        self.BaseUrl = base_url
        self.Username = username
        self.Password = password
        self.ProjectID = projectid

    def login(self):
        """
        Perform post request with credentials in json format in order to receive MSTR-AuthenticationToken
        """
        login_data = {
            'username': self.Username,
            'password': self.Password,
            'loginMode': 1
        }

        RequestResponse = requests.post(self.BaseUrl + 'auth/login', data=login_data)

        if RequestResponse.ok:
            AuthenticationToken = RequestResponse.headers['X-MSTR-AuthToken']
            Cookies = dict(RequestResponse.cookies)
            return AuthenticationToken, Cookies

        else:
            print('HTTP_REQUEST FAILED %s' % RequestResponse.status_code)
            pass

    def set_base_headers(self, authToken):
        """
        Set base headers for future calls
        """
        base_headers = {
            "X-MSTR-AuthToken": authToken,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-MSTR-ProjectID": self.ProjectID
        }
        return base_headers

    def get_cube_information(self, header, cookies, cube_id):
        """
        Return Cube Information by given Cube ID
        """
        response = requests.get(self.BaseUrl +
                                'cubes/' + cube_id,
                                headers=header,
                                cookies=cookies)

        return response

    def json_to_dataframe(self, response):
        """
        Turns semi-structured json response into relational PandasSeries tabulary format
        """
        transformed_response = response.json()
        df = pd.DataFrame(transformed_response)

        return df

    # Iterational Part to return all cubes in Project by given Root Folder
    # -------------------------------------------------------------------------------------------------------------------------#

    def get_folder_content(self, headers, cookies, folder_id):
        """
        Receive Objects inside folder by folder_id
        """
        response = requests.get(self.BaseUrl + 'folders/' + folder_id,
                                headers=headers,
                                cookies=cookies)

        return response

    def get_folder_id(self, dataframe):
        """
        Get all Ids of Object "Folder"
        """
        folders = []

        for row in dataframe.iterrows():

            if row[1][2] == 8:
                folders.append(row[1][1])

            else:
                pass

        return folders

    def folder_iteration(self, header, cookies, root):
        """
        Return contents of all folders from given root folder
        """
        folder_ids = [root]
        temporary_storage_list = []

        while folder_ids:

            for folder_id in folder_ids:
                data = self.get_folder_content(headers=header,
                                               cookies=cookies,
                                               folder_id=folder_id)

                df = self.json_to_dataframe(data)

                second_level_folders = self.get_folder_id(df)

                folder_ids = second_level_folders

                temporary_storage_list.append(df)

        df_final = pd.concat(temporary_storage_list)

        return df_final