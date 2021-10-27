#%%
import time
import argparse
from LibraryGenesis import LibraryGenesisScraper

from dewey_category_check import make_list_of_ddc_categories

#%%
def run_library_parse(start_year, end_year, language, starting_limit, max_limit):
    start_time = time.perf_counter()
    libgen_scraper = LibraryGenesisScraper()
    #%%
    for year in range(start_year, end_year):
        s_year = year
        l_year = year + 1
        limit1 = starting_limit
        limit2 = max_limit
        print(f'Start year: {s_year} --- End year: {l_year}')
        while True:
            print(f'limit1: {limit1} --- limit2: {limit2}')
            max_callable = libgen_scraper.JSON_response(start_year=s_year,
                                                        last_year=l_year,
                                                        limit1=limit1,  # limit1 controls the start point
                                                        limit2=limit2)  # limit2 controls the max number of responses
            if max_callable == limit2:
                libgen_scraper.initialise_dataframe()
                libgen_scraper.filter_dataframe_year(start_year, end_year)
                libgen_scraper.filter_dataframe_language(language)
                libgen_scraper.get_download_urls()
                libgen_scraper.get_ddc()
                libgen_scraper.filter_categories(category_list=make_list_of_ddc_categories([000, 500, 600]))
                libgen_scraper.add_dataframe_to_register()
                d_end_time = time.perf_counter()
                print(f'script took {d_end_time - start_time} second(s) to load JSON data into csv')
                limit1 += max_limit
            else:
                break

def run_library_upload_download(download_location):
    #%%
    start_time = time.perf_counter()
    libgen_scraper = LibraryGenesisScraper()
    libgen_scraper.get_files_from_site(download_location)
    end_time = time.perf_counter()
    print(f'script took {end_time - start_time} second(s) to finish')


if __name__ == '__main__':
    FUNCTION_MAP = {'parse_library': run_library_parse, 'download_library': run_library_upload_download}
    parser = argparse.ArgumentParser(description='This script downloads pdf documents from "http://libgen.rs/" '
                                                 'and uploads them to either s3 bucket or your pc depending on option argument')
    parser.add_argument('command', choices=FUNCTION_MAP.keys(), help='The "parse_library" command takes the data '
                                                                     'from libgen url and places it into '
                                                                     'Book-Register.csv. \n\n'
                                                                     'The "download_library" command takes the url from '
                                                                     '"Book-Register.csv", from there there are 3 choices:\n'
                                                                     '0=upload pdf to bucket,\n'
                                                                     '1=download pdf to local pc,\n'
                                                                     '2=download pdfs from bucket to local pc.')
    parser.add_argument('-sy', '--start_year',
                        default=2018,
                        type=int,
                        help='The start year you are looking to search-(default: 2018)')
    parser.add_argument('-ey', '--end_year',
                        default=2019,
                        type=int,
                        help='The last year you are looking to search-(default: 2019)')
    parser.add_argument('-l', '--language',
                        default='english',
                        type=str,
                        help='The language of the documents you wish to download-(default: english)')
    parser.add_argument('-sl', '--starting_limit',
                        default=1,
                        type=int,
                        help='Set the starting point when searching for documents-(default: 1)')
    parser.add_argument('-ml', '--max_limit',
                        default=10000,
                        type=int,
                        help='Set max number data-points loaded at one time as well as how much '
                             'the starting limit is incremented-(default: 10,000)')
    parser.add_argument('-dl', '--download_location',
                        default=0,
                        type=int,
                        help='Set the download location for the files-(default: 0):\n'
                             '0=upload pdf to bucket\n'
                             '1=download pdf to local pc PC\n'
                             '2=download pdfs from bucket to local pc')
    args = parser.parse_args()

    run = FUNCTION_MAP[args.command]
    if args.command == 'parse_library':
        run(start_year=args.start_year,
            end_year=args.end_year,
            language=args.language,
            starting_limit=args.starting_limit,
            max_limit=args.max_limit)
    elif args.command == 'download_library':
        run(download_location=args.download_location)
