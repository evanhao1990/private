import pandas as pd
from bs4 import BeautifulSoup
import requests
import ast
import time
import logging

logger = logging.getLogger(__name__)

class Scrape(object):
    """
    Page scraping class

    """

    def __init__(self):
        self.soup = None
        self.connection_error = None

    def get_html(self, url):
        """
        Return the html from a url

        """

        retries = 3

        for i in range(retries):

            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    self.connection_error = None
                    return r.text
            except:
                logging.warning("Failed to establish a connection to page: Trying again ({})".format(i + 1))
                if i + 1 == retries:
                    self.connection_error = 1
                    raise

                else:
                    wait = 30
                    logging.info("Waiting {} seconds".format(wait))
                    time.sleep(wait)

    def make_soup(self, url):
        """
        Make soup object from html

        """

        soup = BeautifulSoup(self.get_html(url), 'lxml')
        return soup

    def set_soup(self, url):
        self.soup = self.make_soup(url)

    def get_burger_menu_category_links(self):
        """
        Return all links to category pages in the burger menu.

        """
        burger_menu = self.soup.find(class_='burger-menu__layers')
        links = burger_menu.find_all(class_='category-navigation__link')
        hrefs = [l['href'] for l in links]

        return hrefs

    def create_start_search_link(self, homepage, language_code):
        """
        Create the star search link

        """

        s = '/search?=*&lang={}/'.format(language_code)
        homepage = homepage.replace('/home', '')
        return ["{}{}".format(homepage, s)]

    def get_max_pages(self):
        """
        Get the max number of pages inside a link.

        """

        try:
            button = self.soup.find(class_='paging-controls__page-number-text')
            ints = max([int(s) for s in button.text.split() if s.isdigit()])
        except:
            ints = 1

        return ints

    def check_is_plp_page(self, url):
        """
        Check whether the URL is a plp pages.

        """

        grid_type_1 = self.soup.find(class_='isotope-grid js-isotope-grid')
        grid_type_2 = self.soup.find(class_='plp-content js-plp-content')
        # JJ
        grid_type_3 = self.soup.find(class_='isotope-grid isotope-grid--4-tiles js-isotope-grid')

        if not (grid_type_1 or grid_type_2 or grid_type_3):
            # print "{} is not a plp page".format(url)
            return 1
        else:
            return 0

    def scrape(self):
        """
        Scrape a page

        """

        def get_grid_elements():
            """
            Determine which element to use to find the product grid.

            Can be class='isotope-grid js-isotope-grid' or class='plp-content js-plp-content'

            """

            grid_type_1 = self.soup.find(class_='isotope-grid js-isotope-grid')
            grid_type_2 = self.soup.find(class_='plp-content js-plp-content')
            # JJ
            grid_type_3 = self.soup.find(class_='isotope-grid isotope-grid--4-tiles js-isotope-grid')


            if grid_type_1:
                try: # Sometimes grid can be found but products not found.
                    grid = self.soup.find(class_='isotope-grid js-isotope-grid')
                    products = grid.find_all(class_='isotope-grid__item isotope-grid__item--product')
                except:
                    grid, products = None, None

            elif grid_type_2:
                try:
                    grid = self.soup.find(class_='plp-content js-plp-content')
                    products = grid.find_all(class_='plp__products__item')
                except:
                    grid, products = None, None

            elif grid_type_3:
                try:
                    grid = self.soup.find(class_='isotope-grid isotope-grid--4-tiles js-isotope-grid')
                    products = grid.find_all(class_='isotope-grid__item isotope-grid__item--product')
                except:
                    grid, products = None, None

            else:
                grid, products = None, None

            return grid, products

        def literal_eval(ds):
            """
            Convert string representation of dict into dict.


            Notes:

            - ast will fail unless all values are quoted correctly. I found an occasion where
            {"subrand":null, "price":"null"} - a mix of null and "null" were used, throwing an error.

            To get rid of this, convert all null to "null" using str.replace()

            """

            # Perform the null replace (see notes in docstring)
            a = ds.replace('''"null"''', 'null')
            b = a.replace('null', '''"null"''')
            c = ast.literal_eval(b)

            return c

        # Initialize df
        frames = []

        # Get product grid + product list
        grid, products = get_grid_elements()

        if not products:
            # print "No products found on the page"
            return None

        else:

            for k, product in enumerate(products):

                # Get the product tile
                try: product_tile = product.find(class_='product-tile')
                except: product_tile = None

                # Get the data-layer element
                try: data_layer = product_tile['data-layer-impression']
                except: data_layer = None

                if data_layer is None:
                    logging.warning("data layer attribute not found for product in position {}".format(k))
                else:
                    # Get SKU attached to this product
                    id = product_tile['data-itemid']

                    # Create df of data-layer
                    df = pd.DataFrame(literal_eval(data_layer), index=[id])

                    # Get the position on page
                    df['position_on_page'] = int(product['data-product-grid-position'])

                    # Which image was displayed?
                    img_src = product_tile.find(class_='plp__products__item__image')['data-src']

                    try:
                        img_displayed = "{}.jpg".format(img_src.split("large/")[1].split(".jpg")[0]) # img name
                        pos = img_displayed.rfind('_', 0, img_displayed.rfind('_'))  # (str, start, end)
                        sopt = img_displayed[:pos] # StyleOptionID

                    except:
                        img_displayed = "na"
                        sopt = 'na'

                    df['img_displayed'] = img_displayed
                    df['styleoptionid'] = sopt

                    frames.append(df)

            df = pd.concat(frames, axis=0, sort=False)
            df = df.reset_index(drop=True)

            return df



