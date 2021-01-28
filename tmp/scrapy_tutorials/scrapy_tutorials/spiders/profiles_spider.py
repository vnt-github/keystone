import scrapy
import numpy as np
from os import walk
from string import ascii_uppercase

class ProfilesSpider(scrapy.Spider):
    name = "profiles"
    path_prefix = '/home/vbharot/vnt_rog/p/'
    def start_requests(self):
        for alphabet in ["A"]:
            mypath = self.path_prefix + alphabet
            for (dirpath, dirnames, filenames) in walk(mypath):
                for filename in filenames:
                    yield scrapy.Request(url=f'file://{mypath}/{filename}')
    
    def normal_distribution(self, high, low):
        mean, standard_deviation = (high+low)/2, 0.1 # mean and standard deviation
        return np.random.normal(mean, standard_deviation, 1)

    def get_symbol(self, response):
        """
        Return symbol associated
        """
        start_i = response.request.url.rfind('/')
        end_i = response.request.url.rfind('.')
        return response.request.url[start_i+1:end_i]

    def extract_low_price(self, response):
        """
        Return extracted low price by from profile
        """
        low_price_selector = response.xpath("//*[contains(text(), '52-Week Low')]/following-sibling::td/tt/text()")
        if not low_price_selector:
            print(f'symbol: {self.get_symbol(response)} missing 52-Week Low')
            return
        return float(low_price_selector.get().strip())

    def extract_high_price(self, response):
        """
        Return extracted high price by from profile
        """
        high_price_selector = response.xpath("//*[contains(text(), '52-Week High')]/following-sibling::td/tt/text()")
        if not high_price_selector:
            print(f'symbol: {self.get_symbol(response)} missing 52-Week High')
            return
        return float(high_price_selector.get().strip())

    def parse(self, response):
        try:
            low_price = self.extract_low_price(response)
            if not low_price: return
            high_price = self.extract_high_price(response)
            if not high_price: return
            mid_price = (low_price+high_price)/2
            normal_price = self.normal_distribution(high_price, low_price)[0]
            print(f'symbol: {self.get_symbol(response)}\t52-Week Low: {low_price}\t52-Week High: {high_price}\trandom price: {normal_price:.3f}\tmid price: {mid_price:.3f}')
        except Exception as err:
            print('err', err)

# run: scrapy crawl profiles --nolog
