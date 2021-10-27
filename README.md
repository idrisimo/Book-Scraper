
# PDF Document Library Scraper

## Description
This script scrapes pdf books from library genesis (with plans to add functionality for other sites in the future).
These documents are uploaded directly to an s3 bucket without it being downloaded to the pc first.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install requirements.txt.

```bash
pip install -r requirements.txt
```
This script is also running off of Python 3.8.

## Usage
There are two main commands you can use with this script, each with their own optional arguments

### parse_library
```python3 main.py parse_library```

This command will grab the data on documents available in the libgen website. It is then added to a csv file.
You can give different parameters to filter the books you want:

--start_year : this is the year you are looking for books from.

--end_year : this is the year you are looking for books to.

--language : this is to only get books of that language. (This will need to be typed in lower case and 
currently can only do one language at a time)

For example: ```python3 main.py parse_library -sy 2018 --end_year 2021 -l english```

There are 2 other optional arguments for this command that deal with the api for libgen.rs

--starting_limit : Selects the starting point within the list of books in the websites database.

--max_limit : This does two things.
1) It sets how many data points are called in each iteration. 
2) it sets the new starting limit for the next iteration.

For example:
lets say there are 200,000 books within the years 2018-2019. The starting limit would start you at the first book of that list, 
with the max limit taking the first 10,000 items, processing them, then would add itself to the starting limit. 
It will then start the process again from item number 10,001.
It is recommended that these numbers are left at their defaults (1 and 10,000 respectively) in order to not have a hit on performance.


### download_library
```python3 main.py parse_library```

There is currently only one optional argument for this command.

--download_library : You enter an integer ranging between 0 and 2 selecting one of 3 options

0 - upload pdf to the s3 bucket

1 - download pdf to your local machine, 

2 - download pdfs from bucket to your local machine.

For example: ```python main.py download_library -dl 1``` will download the file directly to your machine from the website.


You can also type ```python main.py --help``` to see the commands below:

```bash
positional arguments:
  {parse_library,download_library}
                        The "parse_library" command takes the data from libgen url and places it into Book-Register.csv. 
                        
                        The "download_library" command takes the url from "Book-Register.csv", 
                        from there there are 3 choices: 
                        0=upload pdf to bucket, 
                        1=download pdf to local pc, 
                        2=download pdfs from bucket to local pc.

optional arguments:
  -h, --help            show this help message and exit
  -sy START_YEAR, --start_year START_YEAR
                        The start year you are looking to search-(default: 2018)
  -ey END_YEAR, --end_year END_YEAR
                        The last year you are looking to search-(default: 2019)
  -l LANGUAGE, --language LANGUAGE
                        The language of the documents you wish to download-(default: english)
  -sl STARTING_LIMIT, --starting_limit STARTING_LIMIT
                        Set the starting point when searching for documents-(default: 1)
  -ml MAX_LIMIT, --max_limit MAX_LIMIT
                        Set max number data-points loaded at one time as well as how much the starting limit is incremented-(default: 10,000)
  -dl DOWNLOAD_LOCATION, --download_location DOWNLOAD_LOCATION
                        Set the download location for the files-(default: 0): 
                        0=upload pdf to bucket 
                        1=download pdf to local pc PC 
                        2=download pdfs from bucket to local pc
                        
```

The script already has default values for the above however you can enter additional arguments if you want to change the settings.

To use the above commands you will need to type the following: ```python3 main.py {command} {-optional arg} {value}```

### Addition usage notes
This script uses threadpooling. if you the script is taking up too much of your pc's resources, it would be advisable to reduce the number of max workers in LibraryGenesis.py

## Support
If there are any bugs you find. please submit an issue ticket via [Github](https://github.com/idrisimo/Book-Scaper). If you want to edit a project, just send me a message there as well or contact me via my email: idrissilva@hotmail.com

## Roadmap
- Add functionality for different download sources.
- import efficiency revolving around memory
## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[TOBEADDED](https://choosealicense.com/licenses/mit/)
