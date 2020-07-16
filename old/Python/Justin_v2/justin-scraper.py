import pandas as pd
import os
import ast
import datetime
from ConfigParser import RawConfigParser
import argparse
from random import shuffle
import logging

# Custom import
from utilities.scrape import Scrape
from utilities.database import InteractWithDB

# Console settings
pd.set_option('display.width', 200)

def get_justin_directory():
    return os.path.dirname(os.path.realpath(__file__))

def get_timestamp():
    """
    Creates a timestamp

    """
    return datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def configure_logging():
    """
    Configure the log process

    """
    justin_dir = get_justin_directory()
    filename = os.path.join(justin_dir, "{}_justin_runtime.log".format(get_timestamp()))

    logging.basicConfig(filename=filename, level=logging.INFO,
                        format='%(levelname)s: %(asctime)s: %(lineno)d: %(funcName)s: %(message)s',
                        datefmt='%Y/%m/%d %I:%M:%S')

    # Logging to stdout
    print_window = logging.StreamHandler()
    print_window.setLevel(logging.INFO)
    print_window.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s: %(lineno)d: %(funcName)s: %(message)s'))
    logging.getLogger('').addHandler(print_window)

def create_settings(brands, countries):

    site_language = {'de': 'de_DE',
                     'nl': 'nl_NL',
                     'dk': 'da_DK'}

    settings = []

    for b in brands:
        for c in countries:
            settings.append((b, c, site_language[c]))

    return settings

def read_config():
    """
    Read in project configuration parameters

    """

    config_path = os.path.dirname(os.path.realpath(__file__))
    config = RawConfigParser()
    config.read(os.path.join(config_path, "scraper_settings.ini"))
    brands = config.get('settings', 'brands')
    countries = config.get('settings', 'countries')

    return ast.literal_eval(brands), ast.literal_eval(countries)

def get_homepage(country, brand):
    """
    Dictionary containing all hompeages

    """

    country = country.lower()

    generic_homepages = {'bs': 'https://shop.bestseller.com',
                         'jj': 'https://www.jackjones.com',
                         'sl': 'https://www.selected.com',
                         'on': 'https://www.only.com',
                         'vl': 'https://www.vila.com',
                         'vm': 'https://www.veromoda.com',
                         'pc': 'https://www.pieces.com',
                         'mm': 'https://www.mamalicious.com',
                         'jr': 'https://www.junarose.com',
                         'ni': 'https://www.nameit.com',
                         'oc': 'https://www.objectci.com',
                         'ya': 'https://www.y-a-s.com',
                         'os': 'https://www.onlyandsons.com',
                         'nm': 'https://www.noisymay.com',
                         'bi': 'https://www.bianco.com',
                         'jl': 'https://www.jlindeberg.com'}

    site_language = {'de': 'de',
                     'nl': 'nl',
                     'dk': 'da'}

    homepages = {}
    for key, value in generic_homepages.iteritems():
        language = site_language[country]
        homepages[key] = "{}/{}/{}/{}".format(value, country, language, 'home')

    return homepages[brand]

def get_url_catgegory(url):
    """
    Extract the url category from the url

    """

    return url.split('/')[-2]

def get_url_type(url_category):
    """
    Get the url type - star or catalog

    """

    if 'search?' in url_category:
        return 'star'
    elif url_category == 'na':
        return 'na'
    else:
        return 'catalog'

def get_url_level_type(url):
    """
    Determine whether url is a top-level or sub-level url

    Example: /clothing/new
    clothing = top
    new = sub

    """

    path = url.split('/', 6)[-1]
    path_len = len(path.split('/'))

    if path_len == 3:
        url_type = 'sub'
    elif path_len == 2:
        url_type = 'top'
    else:
        url_type = 'na'

    return url_type

def get_final_structure(df):
    """
    Prepare the dataframe for sandbox write.

    """

    df = df.rename(columns={'articleNumber':'styleno',
                            'price': 'black_price',
                            'salesPrice':'red_price'})

    cols = ['styleno', 'styleoptionid', 'id', 'brand', 'subbrand',
            'category', 'name', 'black_price', 'red_price', 'coupon',
            'page', 'position_on_page', 'running_position', 'img_displayed',
            'url', 'url_type', 'url_level_type', 'brand_site', 'country',
            'language', 'currency', 'timestamp']

    return df[cols]

def get_currency(country):
    """
    Get local currency

    """

    currency = {'de':'EUR',
                'nl':'EUR',
                'dk':'Danish Krone'}

    return currency[country]

def make_connection(scrape_object, url):
    """
    In order to control the flow of the program in the case when we cannot connect to a url we set
    each call to s.set_soup() inside a error handler. If no connection can be made after 3 retries,
    an error is raised, and s.connection_error is set to 1 inside Scrape().

    """

    s = scrape_object
    try:
        s.set_soup(url)
        return 0

    except:
        logging.critical("Maximum retries exceeded")
        max_retries_error = 1 # This is a quick fix to a bug I do not understand. s.connection_error is supposed
        # to control whether or not the connection was succesful, for some cases I saw in the logs
        # that this step is just being skipped and we go straight to this critical error.
        # Thus the s.connection_error is never set to 1, and if not s.connection_error passes.
        # I don't understand how this happens and cannot recreate the error.

        return max_retries_error


configure_logging()

def main():

    country = 'de'
    brand = 'pc'
    language = 'de'
    homepage = get_homepage(country, brand)
    logging.info(homepage)

    # Initialize and collect links
    s = Scrape()
    mc = make_connection(s, homepage) # returns 0 or 1 (success, failure)

    if not s.connection_error and mc is 0:
        category_links = s.get_burger_menu_category_links()
        star_search_link =  s.create_start_search_link(homepage, language_code=language)
        all_links = category_links + star_search_link
        shuffle(all_links)

        for url in all_links:
            logging.info("category url: {}".format(url))

            # Get the url category
            try: url_cat = get_url_catgegory(url)
            except: url_cat = 'na'

            try: url_level_type = get_url_level_type(url)
            except: url_level_type = 'na'

            # Get the url type
            url_type = get_url_type(url_cat)

            # set initial category url
            mc = make_connection(s, url)
            if not s.connection_error and mc is 0:

                if s.check_is_plp_page(url) == 1: # == failure
                    logging.info("active url: {} is not a PLP page".format(url))

                else:
                    # Get pagination
                    max_pages = s.get_max_pages()

                    # Initialize df storage
                    frames = []

                    # Iterate through pages in category - save results to frames
                    for page in range(1, max_pages+1):
                        page_url = url + r'?sz=60&start={}'.format((page-1) * 60)
                        logging.info(r"active url: {}".format(page_url))

                        mc = make_connection(s, page_url)
                        if not s.connection_error and mc is 0:

                            # Scrape the page
                            df = s.scrape()

                            # If page is plp, but empty df will be None.
                            if df is not None:
                                # Drop all scraped items on the site that are not products (no sku attached).
                                logging.info("page {}: dropped {} items (that were not products)".format(page, len(df) - len(df[df['id'] != ''])))
                                df = df[df['id'] != '']

                                # Additional page metrics
                                df['url'] = page_url
                                df['page'] = page
                                df['running_position'] = df['position_on_page'] + ((df['page'] - 1) * 60)
                                frames.append(df)
                            else:
                                logging.warning("active url: {} was empty".format(page_url))

                        else:
                            logging.warning("active url: {} failed - moving to next url".format(page_url))

                    try:
                        # Concat all pages
                        df = pd.concat(frames, axis=0)
                        df = df.reset_index(drop=True)

                        # Add additional url information
                        df['category'] = url_cat
                        df['url_type'] = url_type
                        df['url_level_type'] = url_level_type
                        df['brand_site'] = brand
                        df['country'] = country
                        df['language'] = language
                        df['currency'] = get_currency(country)

                        # Timestamp at runtime
                        df['timestamp'] = timestamp

                        # Store output

                        df.to_csv('test'Y8DE, sep=';', decimal='.', index=False, header=header, mode='a')

                    except:
                        # Generic try/except to catch the max retries error and no frames saved from any page
                        # in this case we just skip and move to the next.
                        pass

if __name__ == '__main__':

    main()