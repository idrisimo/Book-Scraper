import concurrent.futures
import pandas as pd
import numpy as np
import re
import sys
from os.path import abspath
from dewey_category_check import get_dewey_decimal, format_isbn, format_title
from aws_interface import upload_to_bucket, download_from_bucket, clean_file_name
import requests
from datetime import datetime
import time
from urllib.parse import quote
from urllib.request import urlretrieve, urlopen



class LibraryGenesisScraper:
    """
    This Class is used to scrape https://libgen.rs/. Using their api found here: https://forum.mhut.org/viewtopic.php?f=17&t=6874
    getting data from library genesis' database,
    a download link is put together and then the pdf documents are uploaded directly to an s3 bucket.
    """
    def url_maker(self, base_url, **kwargs):
        """Creates the download link that is later used by the 'upload_files' function"""

        cover_url = kwargs['coverurl']
        md5 = kwargs['md5']
        author = kwargs['author']
        title = kwargs['title']
        year = kwargs['year']

        book_link_list = [f'{author} - ', f'{title}', f' ({year})']

        # URL end point has a hard limit on the number of characters(200)
        clean_endpoint = ''.join(book_link_list).replace(';', '_')
        book_link_utf8 = quote(clean_endpoint[:200])

        try:
            id_group = re.findall('[0-9]{1,7}', cover_url)[0]
        except Exception as e:
            id_group = 'FIXISSUE'
            print(f'issue with Book cover_url: {cover_url}--error: {e}')

        download_url = f'{base_url}{id_group}/{md5.lower()}/{book_link_utf8}.pdf'
        return download_url

    def JSON_response(self, limit1, limit2, start_year, last_year):
        """grabs data matching the fields required."""
        fields = ','.join(['id', 'author', 'title', 'year', 'Language', 'md5', 'coverurl', 'identifier'])

        # limit1 controls the start point
        # limit2 controls the max number of responses

        time_first = f'{start_year}-01-01'
        time_last = f'{last_year}-10-14'

        url = f'https://libgen.rs/json.php?fields={fields}&limit1={limit1}&limit2={limit2}&mode=last&timefirst={time_first}&timelast={time_last}'

        response = requests.get(url)
        # Sometimes it takes a few tries to connect hence the loop
        while response.status_code != 200:
            print(f'no connection --status code:{response.status_code}')
            time.sleep(1)
            response = requests.get(url)
        else:
            self.json_parse = response.json()
            return int(len(self.json_parse))


    def initialise_dataframe(self):
        self.initial_df = pd.DataFrame(self.json_parse)
        self.df = self.initial_df
        self.df['year'] = np.floor(pd.to_numeric(self.df['year'], errors='coerce')).astype('Int64')
        self.df['language'] = self.df['language'].str.lower()


    def filter_dataframe_year(self, year_from, year_to):
        self.df = self.df[self.df['year'].between(year_from, year_to)]

    def filter_dataframe_language(self, language):
        self.df = self.df.loc[self.df['language'] == language.lower()]

    def get_download_urls(self):
        time_now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        base_download_url = f'http://31.42.184.140/main/'
        download_links = []
        files_uploaded = []

        for index, row in self.df.iterrows():
            download_url = self.url_maker(base_url=base_download_url,
                                          author=row['author'],
                                          title=row['title'],
                                          year=row['year'],
                                          md5=row['md5'],
                                          coverurl=row['coverurl'])
            download_links.append(download_url)
            files_uploaded.append([False, f'initial link creation [{time_now}]'])

        self.df['download link'] = download_links
        self.df['file uploaded'] = files_uploaded

    def get_ddc(self):
        """This setup the dataframe column for the dewey decimal category column"""

        def get_dcc_via_threading(df_row):
            row = df_row[1]
            try:
                isbn = format_isbn(row['identifier'])

                return get_dewey_decimal(isbn=isbn)
            except Exception as e:

                title = format_title(row['title'])

                return get_dewey_decimal(title=title)
        print('problem')

        # Edit max workers if your pc can candle it. The Higher it it the more parallel tasks will be active. Will affect performance
        df_iterable = self.df.iterrows()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(get_dcc_via_threading, df_iterable)
            ddc = [result for result in results]
        self.df['dewey decimal category'] = ddc

    def filter_categories(self, category_list):
        self.df = self.df.dropna()
        self.df['dewey decimal category'] = np.floor(pd.to_numeric(self.df['dewey decimal category'], errors='coerce')).astype('Int64')
        self.df = self.df[self.df['dewey decimal category'].isin(category_list)]

    def add_dataframe_to_register(self):
        try:
            current_data = pd.read_csv('Book-Register.csv', index_col=[0], dtype={'id': object})
            new_data = pd.concat([current_data, self.df]).drop_duplicates(subset=['id', 'title'], keep='first')
            new_data.to_csv('Book-Register.csv')
            print('file_updated')
        except:
            self.df.to_csv('Book-Register.csv')
            print('file created')

    def update_register(self, dataframe):
        current_data = pd.read_csv('Book-Register.csv', index_col=[0])
        new_data = pd.concat([current_data, dataframe]).drop_duplicates(subset=['id', 'title'], keep='last')
        new_data.to_csv('Book-Register.csv')
        print('file_updated')

    def upload_document_via_threading(self, dataframe_row):
        dataframe_row = dataframe_row[1]

        book_id = dataframe_row['id']
        author = dataframe_row['author']
        title = dataframe_row['title']
        year = dataframe_row['year']
        url = dataframe_row['download link']
        document_name = clean_file_name(f'{str(author)[:100]} - {str(title)[:100]} ({year})')


        # TODO setup environmental variables for bucket access
        upload_result = upload_to_bucket(bucket_name='BUCKET',
                                     key='KEY',
                                     password='PASS',
                                     document_url=url,
                                     document_name=document_name,
                                     document_id=book_id)

        return upload_result

    def download_files_to_pc_via_threading(self, dataframe_row):
        start_time = time.perf_counter()

        retries = 0

        dataframe_row = dataframe_row[1]
        book_id = dataframe_row['id']
        author = dataframe_row['author']
        title = dataframe_row['title']
        year = dataframe_row['year']
        url = dataframe_row['download link']


        document_name = clean_file_name(f'{str(author)[:100]} - {str(title)[:100]} ({year})')
        max_retries = 20
        # creating a connection to the pdf
        print(f"Creating the connection ...{book_id}")
        with requests.get(url, stream=True) as r:
            while r.status_code != 200:
                retries += 1
                sleep_time = 1.2 ** retries
                print(f"Could not download the file '{url}'\n"
                      f"File Name : {document_name}\n"
                      f"Error Code : {r.status_code}\n"
                      f"Reason : {r.reason}\n"
                      f"Retries : {retries}/{max_retries}\n"
                      f"Sleep time in seconds : {sleep_time}\n\n")
                #time.sleep(sleep_time)
                time.sleep(sleep_time)
                if r.status_code != 200 and retries == max_retries:
                    print(f"Could not download the file '{url}'\n"
                          f"File Name : {document_name}\n"
                          f"Error Code : {r.status_code}\n"
                          f"Reason : {r.reason}\n\n Download attempted cancelled",
                          file=sys.stderr)
                    #r.close()
                    break
            else:
                request_time = time.perf_counter()
                print(f'request took {request_time - start_time} second(s) to finish')
                # Storing the file as a pdf
                print(f"Saving the pdf file  :\n\"{document_name}\" ...")
                with open(f'./downloads/{document_name}.pdf', 'wb') as f:
                    try:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                        print(f'PDF Saved : \n \"{document_name}\"')
                        return 1
                        #print(end='\n\n')
                    except Exception as err:
                        print(f"==> Couldn't save : {document_name}\\ \n Error : {err}")
                        #f.flush()
                        #r.close()
                #r.close()
                retries = 0
        end_time = time.perf_counter()
        print(f'download took {end_time - start_time} second(s) to finish')
    def download_to_pc(self):
        dataframe = pd.read_csv('Book-Register.csv', index_col=[0], converters={'file uploaded': pd.eval})

        for index, row in dataframe.iterrows():
            author = row['author']
            title = row['title']
            year = row['year']
            url = row['download link']
            print(url)
            document_name = f'{author} - {title} ({year})'
            r = requests.get(url=url, allow_redirects=True)
            open(f'./downloads/{document_name}.pdf', 'wb').write(r.content)

            # # creating a connection to the pdf
            # print("Creating the connection ...")
            # with requests.get(url, stream=True) as r:
            #     while r.status_code != 200:
            #         retries += 1
            #         print(f"Could not download the file '{url}'\n"
            #               f"File Name : {document_name}\n"
            #               f"Error Code : {r.status_code}\n"
            #               f"Reason : {r.reason}\n"
            #               f"Retries : {retries}/5\n\n",
            #               file=sys.stderr)
            #         time.sleep(2 ** retries)
            #         if r.status_code != 200 and retries == 5:
            #             print(f"Could not download the file '{url}'\n"
            #                   f"File Name : {document_name}\n"
            #                   f"Error Code : {r.status_code}\n"
            #                   f"Reason : {r.reason}\n\n Download attempted cancelled",
            #                   file=sys.stderr)
            #             break
            #     else:
            #         # Storing the file as a pdf
            #         print(f"Saving the pdf file  :\n\"{document_name}\" ...")
            #         with open(f'./downloads/{document_name}.pdf', 'wb') as f:
            #             try:
            #                 total_size = int(r.headers['Content-length'])
            #                 saved_size_pers = 0
            #                 moversBy = 8192 * 100 / total_size
            #                 for chunk in r.iter_content(chunk_size=8192):
            #                     if chunk:
            #                         f.write(chunk)
            #                         saved_size_pers += moversBy
            #                         print(f"\r=>> %.2f%%" % (
            #                             saved_size_pers if saved_size_pers <= 100 else 100.0), end='')
            #                 print(end='\n\n')
            #             except Exception:
            #                 print(f"==> Couldn't save : {document_name}\\")
            #                 f.flush()
            #                 r.close()
            #         r.close()
            #         retries = 0


    def get_files_from_site(self, download_location):
        start_time = time.perf_counter()
        dataframe = pd.read_csv('Book-Register.csv', index_col=[0], converters={'file uploaded': pd.eval})


        if download_location == 0:
            print('Uploading from library to bucket')
            download_function = self.upload_document_via_threading
            download_df = dataframe[dataframe['file uploaded'].str[0] == False]
            max_worker = 20

        elif download_location == 1:
            print('Downloading from library to local pc')
            download_function = self.download_files_to_pc_via_threading
            download_df = dataframe
            max_worker = 3
            # self.download_to_pc()
            # return
        elif download_location == 2:
            print('Downloading from bucket to local pc')
            download_from_bucket()
            return
        else:
            print('Invalid command argument, please use either 0, 1, 2')
        print(f'Number of books that can potentially be downloaded: {download_df.shape[0]}')
        if download_df.shape[0] > 0:
            non_uploaded_df_iter = download_df.iterrows()
            # Edit max workers if your pc can candle it. The Higher it it the more parallel tasks will be active. Will affect performance
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_worker) as executor:
                results = executor.map(download_function, non_uploaded_df_iter)
                if download_location == 0:
                    files_uploaded_list = [result for result in results]
                    download_df['file uploaded'] = files_uploaded_list
                    print(f'Total number uploaded: {len([item for item in files_uploaded_list if item[0] == True])}')
                    self.update_register(download_df)
                elif download_location == 1:
                    files_downloaded = [result for result in results]
                    print(f'Total number downloaded: {len([item for item in files_downloaded if item == 1])}')
                else:
                    print('Files downloading to PC.')
        else:
            print('no new books added to register')

        end_time = time.perf_counter()
        print(f'Total upload time took {end_time - start_time} second(s) to finish')





