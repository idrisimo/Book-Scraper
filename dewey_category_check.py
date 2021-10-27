import requests
import xmltodict
from urllib.parse import quote
import re


def format_isbn(num):
    matches = [x.group() for x in re.finditer(r"\d{10}(\d{3})?|(\d{3}-\d{1}-\d{3}-\d{5}-\d{1})", num)]
    regex = re.sub(r"[^a-zA-Z0-9]+", ' ', matches[0])
    isbn = ''.join(regex.split(' '))
    return isbn


def format_title(string):
    title_no_special = re.sub(r"[^a-zA-Z0-9]+", ' ', string)
    try:
        title_no_s = title_no_special.split(' s ')
        if len(title_no_s[0]) > len(title_no_s[1]):
            title = title_no_s[0]
        else:
            title = title_no_s[1]
        title_clean = ' '.join(title.split(' ')[0:-1])
    except:
        title_clean = title_no_special
    return title_clean


def get_dewey_decimal(**kwargs):
    """scrapes http://classify.oclc.org/classify2/api_docs/classify.html to get the dewey decimal category"""
    # Creates endpoint for url
    for endpoint_key, endpoint_val in kwargs.items():
        if endpoint_key == 'isbn':
            url_endpoint = f'{endpoint_key}={endpoint_val}'
        else:
            title = quote(endpoint_val)
            url_endpoint = f'{endpoint_key}={title}'
    try:
        response = requests.get(f'http://classify.oclc.org/classify2/Classify?{url_endpoint}')
        status_code = response.status_code
        xml_parse = xmltodict.parse(response.content)
        response_code = xml_parse['classify']['response']['@code']
        if response_code == '2':
            print(f'url_endpoint: {endpoint_key}= {endpoint_val}\n'
                  f'--response code: {response_code}')
            try:
                ddc_parse = xml_parse['classify']['recommendations']['ddc']['mostPopular']['@sfa']
                ddc = f'{ddc_parse[0:2]}0'
            except:
                ddc = None
        elif response_code == '4':
            print(f'url_endpoint: {endpoint_key}= {endpoint_val}\n '
                  f'--response code: {response_code}:\n'
                  f'Multple documents found, selecting and trying again.')
            new_endpoint = xml_parse['classify']['works']['work'][0]['@owi']
            second_response = requests.get(f'http://classify.oclc.org/classify2/Classify?owi={new_endpoint}')
            second_xml_parse = xmltodict.parse(second_response.content)
            try:
                second_ddc_parse = second_xml_parse['classify']['recommendations']['ddc']['mostPopular']['@sfa']
                print(f'url_endpoint: {endpoint_key}= {endpoint_val}\n '
                      f'--response code: {second_response}')
                ddc = f'{second_ddc_parse[0:2]}0'
            except:
                ddc = None
        elif response_code == '100':
            print(f'url_endpoint issue: {endpoint_key}= {endpoint_val}\n'
                  f'--HTTP status code: {status_code}\n'
                  f'--response code: {response_code}:\n'
                  f'No input. The method requires an input argument.')
            ddc = None

        elif response_code == '101':
            print(f'url_endpoint issue: {endpoint_key}= {endpoint_val}\n'
                  f'--HTTP status code: {status_code}\n'
                  f'--response code: {response_code}:\n'
                  f'Invalid input. The standard number argument is invalid.')
            ddc = None

        elif response_code == '102':
            print(f'url_endpoint issue: {endpoint_key}= {endpoint_val}\n'
                  f'--HTTP status code: {status_code}\n'
                  f'--response code: {response_code}:\n'
                  f'Not found. No data found for the input argument')
            ddc = None

        elif response_code == '200':
            print(f'url_endpoint issue: {endpoint_key}= {endpoint_val}\n'
                  f'--HTTP status code: {status_code}\n'
                  f'--response code: {response_code}:\n'
                  f'Unexpected error.')
            ddc = None

    except KeyError as key_err:
        print(f'url_endpoint issue: {endpoint_key}= {endpoint_val}\n'
              f'--HTTP status code: {response.status_code}\n'
              f'--Error: {key_err}')
        ddc = None
    return ddc



def make_list_of_ddc_categories(cat_list):
    """makes a list of categories every 10 steps between the start number and the nearest hundred"""
    cat_range = []
    for catnum in cat_list:
        cat_range.append(list(range(catnum, catnum + 100, 10)))
    return [item for sublist in cat_range for item in sublist]