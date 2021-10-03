import requests
import json
import pandas as pd


class UKCompaniesHouseAPI:
    #  Companies House API Class Wrapper
    # GET API URLs
    __url_officer_appointments = 'https://api.company-information.service.gov.uk/officers/{' \
                                 '}/appointments?start_index={}'  # /{transaction_id}/
    __url_company_profile = 'https://api.company-information.service.gov.uk/company/{}'  # /{company_no}/
    __url_company_officers = "https://api.company-information.service.gov.uk/company/{}/officers?start_index={}"  #
    # /{company_no}/
    __url_significant = 'https://api.company-information.service.gov.uk/company/{}/persons-with-significant-control'
    # /{company_no}/
    __url_filing_hist = 'https://api.company-information.service.gov.uk/company/{}/filing-history'  # /{company_no}/
    __url_statements_doc = 'https://api.company-information.service.gov.uk/company/{}/filing-history/{}'  # /{

    # company_no}/

    def __init__(self, apikey):
        self.key = apikey

    def _api_url_args(self, url, search_term, start_index=0, **kwargs):  # constructor function
        if "transaction_id" not in kwargs.keys():
            if start_index == 0:
                response = requests.get(url.format(search_term, start_index), auth=(self.key, ''))
                if response.status_code == 200:  # HTTP status codes Headers returned OK
                    search_result = json.JSONDecoder().decode(response.text)
                    return search_result, response.status_code
                else:
                    search_result = (
                        f"HTTP status error code: {response.status_code} ***** "f"Reason: {response.reason}")
                return search_result, response.status_code
            else:
                response = requests.get(url.format(search_term, start_index), auth=(self.key, ''))
                if response.status_code == 200:  # HTTP status codes Headers returned OK
                    search_result = json.JSONDecoder().decode(response.text)
                    return search_result, response.status_code
                else:
                    search_result = (
                        f"HTTP status error code: {response.status_code} ***** "f"Reason: {response.reason}")
                return search_result, response.status_code
        else:
            statement_id = kwargs.get("transaction_id")
            response = requests.get(url.format(search_term, statement_id), auth=(self.key, ''))
            if response.status_code == 200:  # HTTP status codes Headers returned OK
                search_result = json.JSONDecoder().decode(response.text)
                return search_result, response.status_code
            else:
                search_result = f"HTTP status error code: {response.status_code} ***** "f"Reason: {response.reason}"
            return search_result, response.status_code

    def officer_appointments(self, appointment_id):
        """
        Function returns the officer appointments from first to last page
        """
        off_appntmnt = pd.DataFrame()  # dataframe for appending search results items
        response, response_code = self._api_url_args(self.__url_officer_appointments, appointment_id)
        if response_code == 200:
            if response['total_results'] > 35:  # more than one page of appointments
                pages = response['total_results'] // 35  # QUOTIENT of 35 indicates the pages
                result_leftover = response['total_results'] % 35  # MOD of 35 indicates a remainder division (0 or more)
                if pages >= 1 and result_leftover > 0:
                    pages += 1  # add the mod of total result of officer appointments plus one for if there was a
                    # remainder
                    for i in range(pages):  # loop through the range from 0 to last page of officer appointments
                        if i > 0:  # for start index 0 (first page) start at else statement, next page first statement
                            response, response_code = self._api_url_args(self.__url_officer_appointments,
                                                                         appointment_id, start_index=(35 * i) + 1)
                        else:
                            response, response_code = self._api_url_args(self.__url_officer_appointments,
                                                                         appointment_id)
                        for data in response['items']:
                            df = pd.json_normalize(data)
                            off_appntmnt = off_appntmnt.append(df)
            else:  # one page result
                response, response_code = self._api_url_args(self.__url_officer_appointments, appointment_id)
                for data in response['items']:
                    df = pd.json_normalize(data)
                    off_appntmnt = off_appntmnt.append(df)
            return off_appntmnt
        else:
            off_appntmnt = pd.DataFrame([[response, response_code]], columns=['Response', 'Status_Code'])
            return off_appntmnt

    def company_significant_controller(self, company_no):
        """
        Function returns the significant controller officer with 75% or more
        """
        df_sig = pd.DataFrame()  # dataframe for appending search results items
        response, response_code = self._api_url_args(self.__url_significant, company_no)
        if response_code == 200:
            for data in response['items']:
                df = pd.json_normalize(data)
                df_sig = df_sig.append(df)
            return df_sig.reset_index()  # create 0, 1, 2... index for the dataframe to be used elsewhere
        else:
            df_sig = pd.DataFrame([[response, response_code]], columns=['Response', 'Status_Code'])
            return df_sig  # use if 'Status_Code' not in significant_controller.keys(): to save into a dataframe in
            # your python program

    def company_profile(self, company_no):
        """
        Function returns company registration details
        """
        response, response_code = self._api_url_args(self.__url_company_profile, company_no)
        if response_code == 200:
            df = pd.json_normalize(response)
            return df
        else:
            return response

    def company_documents(self, company_no):
        """
        Function returns company's statements data using company ID and transaction ID
        """
        id_list = self.company_filing_history(company_no)[0]  # returns the first variable function transaction_id
        for transaction_id in id_list:
            response, response_code = self._api_url_args(self.__url_statements_doc, company_no, transaction_id)
            if response_code == 200:
                # df_company_docs = pd.json_normalize(response)
                link = response['links']['document_metadata']
                # info = response['description']
                amaz_response = requests.get(link, headers={'Accept': 'application/pdf'}, auth=(self.key, ''))
                links_document = json.JSONDecoder().decode(amaz_response.text)
                doc_url = links_document['links']['document']
                document_metadata = requests.get(doc_url, headers={'Accept': 'application/pdf'}, auth=(self.key, ''))
                return document_metadata
            else:
                df_company_docs = pd.DataFrame([[response, response_code]], columns=['Response', 'Status_Code'])
                return df_company_docs

    def company_filing_history(self, company_no):
        """
        Function returns the filing history data details including document transaction ID
        """
        df_filing_hist = pd.DataFrame()  # dataframe for appending search results items
        response, response_code = self._api_url_args(self.__url_filing_hist, company_no)
        if response_code == 200:
            for data in response['items']:
                df = pd.json_normalize(data)
                df_filing_hist = df_filing_hist.append(df)
            return df_filing_hist['transaction_id'], df_filing_hist[
                'description']  # returns a description and transaction ID
        else:
            df_filing_hist = pd.DataFrame([[response, response_code]], columns=['Response', 'Status_Code'])
            return df_filing_hist

    def company_officers(self, company_no):
        """
        Function returns a list of officers in the company
        """
        df_officers = pd.DataFrame()  # dataframe for appending search results items
        response, response_code = self._api_url_args(self.__url_company_officers, company_no)
        if response_code == 200:
            if response['total_results'] > 35:
                pages = response['total_results'] // 35  # QUOTIENT of 35 indicates the pages
                result_leftover = response[
                                      'total_results'] % 35  # MOD of 35 indicates a remainder division (0 or more)
                if pages >= 1 and result_leftover > 0:
                    pages += 1  # count the mod of total result of officers per page plus one for if there was a
                    # remainder
                    for i in range(pages):  # loop through the range from 0 to last page of officers
                        if i > 0:
                            response, response_code = self._api_url_args(self.__url_company_officers, company_no,
                                                                         start_index=(35 * i) + 1)
                        else:
                            response, response_code = self._api_url_args(self.__url_company_officers, company_no)
                        for officers_data in response['items']:
                            df_tmp = pd.json_normalize(officers_data)
                            df_officers = df_officers.append(df_tmp)
            else:
                response, response_code = self._api_url_args(self.__url_company_officers, company_no)
                for data in response['items']:
                    df = pd.json_normalize(data)
                    df_officers = df_officers.append(df)
            return df_officers
        else:
            df_officers = pd.DataFrame([[response, response_code]], columns=['Response', 'Status_Code'])
            return df_officers

    def check_significant_controller_with_officer(self, appointment_id, entity_name=""):
        df_chk_sig_contrl_off = pd.DataFrame()
        company_no_list = []  # empty list to append company numbers to a dataframe column at the end
        appointments = self.officer_appointments(appointment_id)
        for company_no in appointments['appointed_to.company_number']:
            significant_controller = self.company_significant_controller(company_no)
            if 'Status_Code' not in significant_controller:
                if entity_name != "":  # non empty search for entity_name
                    for rng in range(
                            len(significant_controller.index)):  # add multiple company no rows if there are more
                        # than one shareholder
                        if entity_name in significant_controller['name'][rng]:
                            df_chk_sig_contrl_off = df_chk_sig_contrl_off.append(
                                significant_controller[rng:rng + 1])  # select the specific row where the name is found
                            company_no_list.append(company_no)
                else:  # empty search term for entity_name
                    df_chk_sig_contrl_off = df_chk_sig_contrl_off.append(significant_controller)
                    for rng in range(
                            len(significant_controller.index)):  # add multiple company number rows if there are more
                        # than one shareholder
                        company_no_list.append(company_no)
        df_chk_sig_contrl_off['company_no'] = company_no_list
        return df_chk_sig_contrl_off


# boilerplate
if __name__ == '__main__':
    api_key = 'fc955fbf-db64-43a5-8c86-d233311c80a2' #  dummy API key
    main_api = UKCompaniesHouseAPI(api_key)
    print(main_api.company_profile('00445790'))
