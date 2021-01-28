import scrapy
import numpy as np
from os import walk
from string import ascii_uppercase

class ProfilesSpider(scrapy.Spider):
    name = "profiles"
    path_prefix = '/home/vbharot/vnt_rog/p/'
    def start_requests(self):
        for alphabet in ascii_uppercase:
            mypath = self.path_prefix + alphabet
            for (dirpath, dirnames, filenames) in walk(mypath):
                for filename in filenames:
                    yield scrapy.Request(url=f'file://{mypath}/{filename}')
        # for url in urls:
        #     # TODO: revise yield
        #     yield scrapy.Request(url=url, callback=self.parse)
    # start_urls = [
    #         # 'https://quotes.toscrape.com/page/1/',
    #         # 'https://quotes.toscrape.com/page/2/',
    #         'file:///home/vbharot/vnt_rog/p/A/AETH.html'
    #     ]
    
    def normal_distribution(self, high, low):
        mean, standard_deviation = (high+low)/2, 0.1 # mean and standard deviation
        return np.random.normal(mean, standard_deviation, 1)

    def parse(self, response):
        try:
            start_i = response.request.url.rfind('/')
            end_i = response.request.url.rfind('.')
            name = response.request.url[start_i+1:end_i]
            low_price = float(response.xpath("//*[contains(text(), '52-Week Low')]/following-sibling::td/tt/text()").get().strip())
            high_price = float(response.xpath("//*[contains(text(), '52-Week High')]/following-sibling::td/tt/text()").get().strip())
            mid_price = (low_price+high_price)/2
            normal_price = self.normal_distribution(high_price, low_price)[0]
            print('name:', name, ' prices:', mid_price, normal_price)
        except Exception as err:
            print('err', err)

# run: scrapy crawl profiles --nolog
