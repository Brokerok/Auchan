import pickle5 as pickle
from urlextract import URLExtract
import requests
from multiprocessing import Pool
from loguru import logger


logger.add('logs.log', format='{time} :: {level} :: {message}', level='INFO', rotation='5 minutes',
           retention='20 minutes')

file_load = pickle.load(open("messages_to_parse.dat", "rb"))
dict2 = {}


def parsing_url(data):
    extractor = URLExtract()
    urls = []
    for i in data:
        url = extractor.find_urls(i)
        if url != []:
            if 'http' in url[0]:
                urls.append(url[0])
    logger.info('parsing url done')
    return urls


def check_availability(urls_list):
    dict1 = {}
    for url in urls_list:
        try:
            request = requests.head(url, timeout=5)
            dict1[url] = request.status_code
            logger.info(f'{url} - available')
        except requests.exceptions.Timeout as e:
            logger.error(f'{url} - {str(e)}')
    logger.info('check availability done')
    return dict1


def unshorten(url):
    session = requests.Session()
    resp = session.head(url, allow_redirects=True, timeout=5)
    logger.info(f'{url} - unshorten')
    return resp.url


def print_dict(dict_example):
    logger.info(f'dict print')
    print(dict_example)


if __name__ == '__main__':
    logger.info('program starting')
    dict1 = check_availability(parsing_url(file_load))
    dict2 = dict()
    for url in dict1.keys():
        try:
            res = unshorten(url)
        except:
            request = requests.head(url, timeout=5)
            logger.error(f'{url} - cant unshorten')
            res = request.headers['Location']
        dict2[url] = res
    p = Pool(processes=2)
    p.map(print_dict, [dict1, dict2])
    logger.info('program finish')

