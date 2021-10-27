import boto3
from botocore.exceptions import ClientError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from os.path import abspath
from io import BytesIO
import time
from datetime import datetime
from urllib.parse import quote_plus



def print_s3_inventory(bucket_name, key, password):
    session = boto3.Session(aws_access_key_id=key, aws_secret_access_key=password)
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    book_list = []
    for item in bucket.objects.all():
        if 'ScrapedBooks' in item.key:
            book_location = item.key
            book_list.append(book_location.split('/'))

    book_list_clean = []
    for list in book_list:
        book_list_clean.append(list[1])
    print(len(book_list_clean))
    #print(book_list_clean)
    return book_list_clean


def requests_retry_session(retries=25, backoff_factor=0.3, status_forcelist=(500, 502, 504, 503), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def upload_to_bucket(bucket_name, key, password, document_url, document_name, document_id):
    time_now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                        'ThrottlingException')
    start_time = time.perf_counter()
    print(f'requesting document from register: {document_id}')
    #response = requests.get(document_url)
    try:
        response = requests_retry_session().get(document_url)
    except Exception as err:
        print(err)
        response = None
    request_time = time.perf_counter()
    print(f'request took {request_time - start_time} second(s) to finish')
    retries = 0
    retry = True
    while retry and retries <= 3:
        try:
            print(f'Connecting to bucket...{document_id}')
            session = boto3.Session(aws_access_key_id=key, aws_secret_access_key=password)
            s3 = session.resource('s3')
            bytesIO = BytesIO(response.content)

            print(f'uploading: {document_id}')
            s3.Object(bucket_name, f'ScrapedBooks/{document_name}.pdf').put(Body=bytesIO)

            print(f'Upload complete: {document_id}')
            result = [True, f'document successfully uploaded [{time_now}] {document_id}']
            retries = 0

            break
        except ClientError as cerr:
            if cerr.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                raise
            print(f'(Error:{response.status_code} --  -- {document_id})  Slowdown! Maybe try using fewer workers. Retries = {retries}')
            result = [False, f'document failed to upload [{time_now}] - Error - {cerr}']
            time.sleep(2 ** retries)
            retries += 1
        except AttributeError as aerr:
            print(aerr)
            result = [False, f'document failed to upload [{time_now}] - Error - {aerr}']
            time.sleep(2 ** retries)
            retries += 1
    end_time = time.perf_counter()
    print(f'upload took {end_time - start_time} second(s) to finish {document_id}')
    return result

def clean_file_name(file_name):
    file_name = file_name.replace('(None) - ', '')
    file_name = file_name.replace('.pdf', '')
    file_name = file_name.replace('; - ', '')
    if ';' == file_name[0]:
        file_name = file_name.replace('; ', '', 1)
    chars_to_replace = '\/:*?"<>|;'
    for c in chars_to_replace:
        if c in file_name:
            file_name = file_name.replace(c, '_')
    return file_name

def download_from_bucket():
    start_time = time.perf_counter()
    bucket_name = 'BUCKET'
    print('Connecting to bucket....')
    session = boto3.Session(aws_access_key_id='KEY', aws_secret_access_key='PASS')
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    location_string = 'ScrapedBooks/'
    bucket_book_count = len([book_item for book_item in bucket.objects.all() if location_string in book_item.key])
    print(f'Number of books that can potentially be downloaded: {bucket_book_count}')

    base_url = f'https://{bucket_name}.s3.eu-west-2.amazonaws.com/'
    count = 0
    for item in bucket.objects.all():
        if 'ScrapedBooks' in item.key:
            book_location = item.key
            book = str(book_location.split(location_string, 1)[1])
            download_url = f'{base_url}{location_string}{quote_plus(book)}'
            print('Requesting PDF...')
            try:
                with requests_retry_session(retries=5).get(url=download_url, allow_redirects=True) as response:
                    print(f'Request Success! Downloading: {book}')
                    request_time = time.perf_counter()
                    print(f'request took {request_time - start_time} second(s) to finish')
                    with open(f'./downloads/{book[:200]}.pdf', 'wb') as f:
                        f.write(response.content)
                        f.close()
                    response.close()
                print(f'Downloaded: {book}')
                count += 1
            except Exception as err:
                print(f'Unable to download: {response.status_code}\n{err}')
    end_time = time.perf_counter()
    print(f'download took {end_time - start_time} second(s) to finish')
    print(f'Total number of books downloaded to your local machine: {count}\nDownload Location: {abspath("./downloads")}')
