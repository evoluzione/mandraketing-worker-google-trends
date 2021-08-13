import pandas as pd
import psycopg2 as pg
import _locale, sqlite3, googleads, traceback, time, os, sys, psycopg2
from googleads import adwords
from googleads import oauth2

import toml
data_toml = toml.load("config.toml")

site_url = data_toml['site_url']
n_keywords = data_toml['n_keywords']
site = data_toml['site_url']
host = data_toml['host']
database = data_toml['database']
user = data_toml['user']
password = data_toml['password']
port= data_toml['port']
postgre_complete_url = data_toml['postgre_complete_url']


def select_keyword():
    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port= port
    )
    cur = conn.cursor()
    global keywords_list_db
    keywords_list_db = []
    for x in range(0, n_keywords):
        cur.execute("SELECT keyword FROM wtforecast WHERE site = %s AND ga_parsed = 0;", (site_url,))
        data = cur.fetchall()
        #print(type(data))
        global keyword
        keyword = data[0]
        keyword = ''.join(keyword)
        keywords_list_db.append(keyword)
        cur.execute("Update wtforecast set ga_parsed = 1 where KEYWORD = %s AND site = %s", (keyword,site_url,))
    conn.commit()
    conn.close()

def check_record_db():
    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port= port
    )
    cur = conn.cursor()
    cur.execute("SELECT keyword FROM wtforecast WHERE site = %s AND ga_parsed = 0;", (site_url,))
    data = cur.fetchall()
    global numbers_kw
    numbers_kw = len(data)
    print(numbers_kw)
    conn.commit()
    conn.close()

locations_id = '2380' #Italia IT

_locale._getdefaultlocale = (lambda *args: ['it_IT', 'UTF-8'])

class searchVolumePuller ( ):
    def __init__(self, client_ID, client_secret, refresh_token, developer_token, client_customer_id):
        self.client_ID = client_ID
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.developer_token = developer_token
        self.client_customer_id = client_customer_id

    def get_client(self):
        access_token = oauth2.GoogleRefreshTokenClient (self.client_ID,
                                                        self.client_secret,
                                                        self.refresh_token)
        adwords_client = adwords.AdWordsClient (self.developer_token,
                                                access_token,
                                                client_customer_id=self.client_customer_id,
                                                cache=googleads.common.ZeepServiceProxy.NO_CACHE)

        return adwords_client

    def get_service(self, service, client):

        return client.GetService (service)

    def get_search_volume(self, service_client, keyword_list):
        # empty dataframe to append data into and keywords and search volume lists#
        keywords = []
        search_volume = []
        categories = []
        competitions = []
        average_cpc = []
        targeted_monthly_searches = []
        keywords_and_search_volume = pd.DataFrame ( )
        # need to split data into smaller lists of 700#
        sublists = [keyword_list[x:x + 700] for x in range (0, len (keyword_list), 700)]
        for sublist in sublists:

            # Construct selector and get keyword stats.
            selector = {
                'ideaType': 'KEYWORD',
                'requestType': 'STATS',
            }

            # select attributes we want to retrieve#
            selector['requestedAttributeTypes'] = [
                'KEYWORD_TEXT',
                'SEARCH_VOLUME',
                'COMPETITION',
                'CATEGORY_PRODUCTS_AND_SERVICES',
                'AVERAGE_CPC',
                'TARGETED_MONTHLY_SEARCHES',
                #'IDEA_TYPE'
                ]

            # configure selectors paging limit to limit number of results#
            offset = 0
            selector['paging'] = {
                'startIndex': str (offset),
                'numberResults': str (len (sublist))
            }

            # specify selectors keywords to suggest for#
            selector['searchParameters'] = [{
                'xsi_type': 'RelatedToQuerySearchParameter',
                'queries': sublist
            }]
            
            # Location setting (optional).
            selector['searchParameters'].append({
                'xsi_type': 'LocationSearchParameter',
                'locations': [{'id': f'{locations_id}'}]
            })

            # Network search parameter (optional)
            selector['searchParameters'].append({
                'xsi_type': 'NetworkSearchParameter',
                'networkSetting': {
                    'targetGoogleSearch': True,
                    'targetSearchNetwork': False,
                    'targetContentNetwork': False,
                    'targetPartnerSearchNetwork': False
                }
            })

            '''
            selector['searchParameters'].append({
                'matchType': 'EXACT'
            })
            '''

            # pull the data#
            page = service_client.get(selector)
            # print(page)
            # access json elements to return the suggestions#
            for i in range (0, len (page['entries'])):
                #keywords
                keywords_from_json= page['entries'][i]['data'][0]['value']['value']
                #print(keywords_from_json)
                keywords.append(keywords_from_json)
                #categorie
                categories_from_json= page['entries'][i]['data'][1]['value']['value']
                #print(categories_from_json)
                categories.append(categories_from_json)
                #competition
                competitions_from_json= page['entries'][i]['data'][2]['value']['value']
                #print(competitions_from_json)
                competitions.append(competitions_from_json)
                #Volume di riceca mensile
                targeted_monthly_searches_from_json= page['entries'][i]['data'][3]['value']['value']
                #print(targeted_monthly_searches_from_json)
                targeted_monthly_searches.append(targeted_monthly_searches_from_json)
                #cpc medio
                microAmount = 'microAmount'
                try:
                    if microAmount in page['entries'][i]['data'][4]['value']['value']: 
                        average_cpc_from_json= page['entries'][i]['data'][4]['value']['value']['microAmount']
                        # print(average_cpc_from_json)
                        average_cpc.append(average_cpc_from_json)
                except:
                    average_cpc_from_json= 0
                    average_cpc.append(average_cpc_from_json)
                #volume di ricerca
                search_volume_from_json= page['entries'][i]['data'][5]['value']['value']
                #print(search_volume_from_json)
                search_volume.append(search_volume_from_json)

        keywords_and_search_volume['Keywords'] = keywords
        keywords_and_search_volume['Search Volume'] = search_volume
        keywords_and_search_volume['Category'] = categories
        keywords_and_search_volume['Competition'] = competitions
        keywords_and_search_volume['Average CPC'] = average_cpc
        keywords_and_search_volume['Monthly Searches'] = targeted_monthly_searches

        #keywords_and_search_volume['Average CPC'] = average_cpc
        return keywords_and_search_volume

if __name__ == '__main__':
    CLIENT_ID = data_toml['CLIENT_ID_ADS']
    CLIENT_SECRET = data_toml['CLIENT_SECRET_ADS']
    REFRESH_TOKEN = data_toml['REFRESH_TOKEN_ADS']
    DEVELOPER_TOKEN = data_toml['DEVELOPER_TOKEN_ADS']
    CLIENT_CUSTOMER_ID = data_toml['CLIENT_CUSTOMER_ID_ADS']
    timestr = time.strftime('%Y%m%d-%H%M%S')
    #print(timestr)


    check_record_db()
    while numbers_kw != 0:
        try:
            check_record_db()
            if numbers_kw < n_keywords:
                n_keywords = 1
            if numbers_kw == 0:
                print()
                sys.exit()
            select_keyword()
            volume_puller = searchVolumePuller(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, DEVELOPER_TOKEN, CLIENT_CUSTOMER_ID)
            adwords_client = volume_puller.get_client ( )
            targeting_service = volume_puller.get_service ('TargetingIdeaService', adwords_client)
            try:
                kw_sv_df = volume_puller.get_search_volume (targeting_service, keywords_list_db)
                check_record_db()
            except:
                print('blocco temporaneo API')
                for x in keywords_list_db:
                    conn = psycopg2.connect(
                        database=database, user=user, password=password, host=host, port= port
                    )
                    cur = conn.cursor()
                    cur.execute("UPDATE wtforecast SET ga_parsed = 0 WHERE keyword = %s AND site = %s", (x,site_url,))
                
                conn.commit()
                conn.close()
                check_record_db()
            #kw_sv_df = pd.json_normalize(kw_sv_df2['Monthly Searches'])
            print(kw_sv_df)


            numbers_of_row = len(kw_sv_df.index)
            print(numbers_of_row)
            for i in range(numbers_of_row):
                print(i)
                df_to_sql = kw_sv_df.iloc[ i , : ]
                #print(df_to_sql)
                #print(type(df_to_sql))
                df_to_sql = list(df_to_sql)
                #print(df_to_sql)

                keyword = list(df_to_sql)[0]
                print(keyword)
                search_volume = list(df_to_sql)[1]
                if str(search_volume) == 'nan':
                    search_volume = 0
                if str(search_volume) == 'None':
                    search_volume = 0
                    print('search volume -----------')
                category_ads = list(df_to_sql)[2]
                competition = list(df_to_sql)[3]
                if str(competition) == 'nan':
                    competition = 0
                if str(competition) == 'None':
                    competition = 0
                    print('competition -----------')
                average_cpc = list(df_to_sql)[4]
                montly_search_volume = list(df_to_sql)[5]


                print('keyword')
                print(keyword)
                print('search_volume')
                print(search_volume)
                print('category_ads')
                print(category_ads)
                print('competition')
                print(competition)
                print('average_cpc')
                print(average_cpc)
                #print('montly_search_volume')
                #print(montly_search_volume)


                conn = psycopg2.connect(database=database, user=user, password=password, host=host, port= port)
                cur = conn.cursor()
                print('------execute')

                cur.execute(f"UPDATE wtforecast SET ga_search_volume = {search_volume}, ga_competition = {competition}, ga_average_cpc = {average_cpc} WHERE keyword = %s AND site = '{site_url}'", (keyword,))
                conn.commit()
                conn.close()


            check_record_db()
            time.sleep(10)
            
        except:
            print('pausa 30 per blocco')
            time.sleep(30)
            pass
