import json

import requests
from requests.auth import HTTPDigestAuth


class ConfigInfo:
    app_key = 'WJZ9t6syN3fXJf4bHcYPcmHCa0lSS3f3'
    url = 'https://api.transpose.io/sql'
    nft_sales_sql = """SELECT contract_address, eth_price, timestamp, log_index, buyer_address
     FROM ethereum.nft_sales ORDER BY timestamp DESC LIMIT 15;"""

    collections_sql = """SELECT contract_address, name, symbol, standard
     FROM ethereum.collections WHERE contract_address = '{}' LIMIT 1;"""

    token_prices_sql = """SELECT timestamp, token_address, price
     FROM ethereum.token_prices ORDER BY timestamp DESC LIMIT 15;"""

    tidb_public_key = '8ngpC8VS'
    tidb_private_key = 'c9b3687f-3462-4450-bc5d-8eeeec31837b'
    tidb_base_url = 'https://eu-central-1.data.tidbcloud.com/api/v1beta/app/dataapp-DJyrkPYY/endpoint'


class RequestInfo:
    def __init__(self):
        self.headers = {
            'Content-Type': 'application/json',
            'X-API-KEY': ConfigInfo.app_key,
        }
        self.url = ConfigInfo.url
        self.nft_tidb = NftInfo()
        self.collection_tidb = CollectionInfo()
        self.token_price_tidb = TokenPriceInfo()

    def get_data(self, sql_query):
        response = requests.post(self.url, headers=self.headers, json={'sql': sql_query})
        return json.loads(response.text)

    def insert_nft_sales(self):
        times = 0
        data = self.get_data(ConfigInfo.nft_sales_sql)
        if data['status'] == 'success':
            for nft_data in data['results']:
                if not self.nft_tidb.get_nft_data(nft_data['contract_address'], nft_data['log_index']):
                    self.nft_tidb.insert_nft_data(
                        nft_data['contract_address'],
                        nft_data['timestamp'],
                        nft_data['log_index'],
                        nft_data['eth_price'],
                        nft_data['buyer_address'],
                    )
                    times += 1
                if not self.collection_tidb.get_collection_data(nft_data['contract_address']):
                    collection_data = self.get_data(ConfigInfo.collections_sql.format(nft_data['contract_address']))
                    if collection_data['status'] == 'success' and len(collection_data['results']) > 0:
                        self.collection_tidb.insert_collection_data(
                            collection_data['results'][0]['contract_address'],
                            collection_data['results'][0]['name'],
                            collection_data['results'][0]['symbol'],
                            collection_data['results'][0]['standard'],
                        )
        return 'Insert {} Data'.format(times)

    def insert_token_prices(self):
        data = self.get_data(ConfigInfo.token_prices_sql)
        print(data)
        times = 0
        if data['status'] == 'success':
            for tokens_data in data['results']:
                if not self.token_price_tidb.get_token_price(tokens_data['token_address'], tokens_data['timestamp']):
                    self.token_price_tidb.insert_token_price(
                        tokens_data['token_address'],
                        tokens_data['timestamp'],
                        tokens_data['price'],
                    )
                    times += 1
        return 'Insert {} Data'.format(times)


class TidbInfo:
    def __init__(self):
        self.get_header = {
            'endpoint-type': 'draft'
        }
        self.post_header = {
            'content-type': 'application/json',
            'endpoint-type': 'draft'
        }
        self.base_url = ConfigInfo.tidb_base_url
        self.public_key = ConfigInfo.tidb_public_key
        self.private_key = ConfigInfo.tidb_private_key

    def insert_request_data(self, data, path):
        url = self.base_url + path
        response = requests.post(url, headers=self.post_header, json=data,
                                 auth=HTTPDigestAuth(self.public_key, self.private_key))
        return json.loads(response.text)

    def get_request_data(self, params, path):
        url = self.base_url + path
        response = requests.get(url, headers=self.get_header, params=params,
                                auth=HTTPDigestAuth(self.public_key, self.private_key))
        return json.loads(response.text)


class NftInfo(TidbInfo):
    def __init__(self):
        super(NftInfo, self).__init__()
        self.insert_endpoint = '/insert_nft'
        self.get_endpoint = '/get_nft'
        self.get_nft_price_endpoint = '/get_nft_price'

    def insert_nft_data(self, contract_address, time_stamp, log_index, eth_price, buyer_address):
        data = {
            'contract_address': contract_address,
            'log_index': log_index,
            'time_stamp': time_stamp,
            'eth_price': eth_price,
            'buyer_address': buyer_address,
        }
        self.insert_request_data(data, self.insert_endpoint)

    def get_nft_data(self, contract_address, log_index):
        params = {
            'contract_address': contract_address,
            'log_index': log_index
        }
        result = self.get_request_data(params, self.get_endpoint)
        if len(result['data']['rows']) > 0:
            return True
        else:
            return False

    def get_nft_price(self, time_interval):
        params = {
            'time_interval': time_interval,
        }
        result = self.get_request_data(params, self.get_nft_price_endpoint)
        result_data = []
        for data in result['data']['rows']:
            result_data.append(data)
        return json.dumps(result_data)


class CollectionInfo(TidbInfo):
    def __init__(self):
        super(CollectionInfo, self).__init__()
        self.insert_endpoint = '/insert_collection'
        self.get_endpoint = '/get_collection'

    def insert_collection_data(self, contract_address, name, symbol, standard):
        data = {
            'contract_address': contract_address,
            'name': name,
            'symbol': symbol,
            'standard': standard,
        }
        self.insert_request_data(data, self.insert_endpoint)

    def get_collection_data(self, contract_address):
        params = {
            'contract_address': contract_address,
        }
        result = self.get_request_data(params, self.get_endpoint)
        if len(result['data']['rows']) > 0:
            return True
        else:
            return False


class TokenPriceInfo(TidbInfo):
    def __init__(self):
        super(TokenPriceInfo, self).__init__()
        self.insert_endpoint = '/insert_token'
        self.get_endpoint = '/get_token'
        self.get_token_endpoint = '/get_price'

    def insert_token_price(self, token_address, time_stamp, price):
        data = {
            'token_address': token_address,
            'time_stamp': time_stamp,
            'price': price,
        }
        print(self.insert_request_data(data, self.insert_endpoint))

    def get_token_price(self, token_address, time_stamp):
        params = {
            'token_address': token_address,
            'time_stamp': time_stamp,
        }
        result = self.get_request_data(params, self.get_endpoint)
        if len(result['data']['rows']) > 0:
            return True
        else:
            return False

    def get_price(self, token_address, days):
        params = {
            'token_address': token_address,
            'days': days,
        }
        result = self.get_request_data(params, self.get_token_endpoint)
        result_data = {
            'label': [],
            'price': []
        }
        for data in result['data']['rows']:
            result_data['label'].append(data['DAY'])
            result_data['price'].append(data['floor_price'])
        return json.dumps(result_data)


if __name__ == '__main__':
    datas = RequestInfo()
    print(datas.insert_nft_sales())





