import scrapy
from os import walk
from string import ascii_uppercase
from scrapy_tutorials.items import Fundamentals

class ProfilesSpider(scrapy.Spider):
    name = "profiles"
    path_prefix = '/home/vbharot/vnt_rog/p/'
    def start_requests(self):
        # for alphabet in ascii_uppercase:
        #     mypath = self.path_prefix + alphabet
        #     for (dirpath, dirnames, filenames) in walk(mypath):
        #         for filename in filenames:
        #             yield scrapy.Request(url=f'file://{mypath}/{filename}')
        yield scrapy.Request(url='file:///C:/Users/farha/Desktop/2011-10/2011/10/profiles/Yahoo/US/01/p/g/GOOG.html')
        # yield scrapy.Request(url='file:///home/vbharot/vnt_rog/p/A/AWK.html')
        # yield scrapy.Request(url='file:///home/vbharot/vnt_rog/p/A/AXSI.html')
        # yield scrapy.Request(url='file:///home/vbharot/vnt_rog/p/A/AWF.html')
        # yield scrapy.Request(url='file:///home/vbharot/vnt_rog/p/A/AXA.html')

    def get_symbol(self, response):
        """
        Return symbol associated
        """
        start_i = response.request.url.rfind('/')
        end_i = response.request.url.rfind('.')
        return response.request.url[start_i+1:end_i]

    @staticmethod
    def convert_str_to_number(num):
        try:
            char_map = {'K':1000, 'M':1000000, 'B':1000000000}
            num = num.replace(',', '')
            if num and num[-1] in char_map.keys():
                return float(num[:-1]) * char_map[num[-1].upper()]
            else:
                return float(num)
        except Exception as err:
            raise err

    def extract_value_from_key_sibling(self, key_selector):
        try:
            #sign = -1 if key_selector.xpath("./following-sibling::td/text()").get() == '-' else 1
            value = key_selector.xpath("./following-sibling::td/text()").get()
            #print(value)
            if not value: return
            return self.convert_str_to_number(value)
        except Exception as err:
            print('sibling extraction err', err)

    def extract_low_price(self, response):
        """
        Return extracted low price by from profile
        """
        low_price_selectors = response.xpath("//*[contains(text(), '52-Week Low')]")
        if not low_price_selectors:
            print(f'symbol: {self.symbol} missing 52-Week Low')
            return
        return self.extract_value_from_key_sibling(low_price_selectors[0])

    def extract_high_price(self, response):
        """
        Return extracted high price by from profile
        """
        high_price_selectors = response.xpath("//*[contains(text(), '52-Week High')]")
        if not high_price_selectors:
            print(f'symbol: {self.symbol} missing 52-Week High')
            return
        return self.extract_value_from_key_sibling(high_price_selectors[0])

    def extract_book_value_mrq(self, response):
        book_value_mrq_selectors = response.xpath("//*[contains(text(), 'Book Value')]")
        if not book_value_mrq_selectors:
            print(f'symbol: {self.symbol} missing Book Value (mrq)')
            return
        return self.extract_value_from_key_sibling(book_value_mrq_selectors[0])

    def extract_earnings_ttm(self, response):
        earnings_ttm_selectors = response.xpath("//*[contains(text(), 'Earnings')]/*[contains(text(), '(ttm)')]/..")
        if not earnings_ttm_selectors:
            print(f'symbol: {self.symbol} missing Earnings (ttm)')
            return

        return self.extract_value_from_key_sibling(earnings_ttm_selectors[0])
   
    def extract_earnings_mrq(self, response):
        earnings_mrq_selectors = response.xpath("//*[contains(text(), 'Earnings')]/*[contains(text(), '(mrq)')]/..")
        if not earnings_mrq_selectors:
            print(f'symbol: {self.symbol} missing Earnings (mrq)')
            return

        return self.extract_value_from_key_sibling(earnings_mrq_selectors[0])

    def extract_daily_volume(self, response):
        daily_volume_selectors = response.xpath("//*[contains(text(), 'Avg Vol')]")
        if not daily_volume_selectors:
            print(f'symbol: {self.symbol} missing Daily Volume (3-month avg)')
            return
        return self.extract_value_from_key_sibling(daily_volume_selectors[0])


    def extract_shares_outstanding(self, response):
        shares_outstanding_selectors = response.xpath("//*[contains(text(), 'Shares Outstanding')]")
        if not shares_outstanding_selectors:
            print(f'symbol: {self.symbol} missing Shares Outstanding')
            return
        return self.extract_value_from_key_sibling(shares_outstanding_selectors[0])


    def parse(self, response):
        try:
            fundamentals = Fundamentals()
            fundamentals['symbol'] = self.get_symbol(response)
            self.symbol = fundamentals['symbol']
            fundamentals['low_price'] = self.extract_low_price(response)
            fundamentals['high_price'] = self.extract_high_price(response)
            fundamentals['shares_outstanding'] = self.extract_shares_outstanding(response)

            fundamentals['daily_volume'] = self.extract_daily_volume(response)
            
            fundamentals['book_value_mrq'] = self.extract_book_value_mrq(response)
            
            #fundamentals['earnings_ttm'] = self.extract_earnings_ttm(response)
            #fundamentals['earnings_mrq'] = self.extract_earnings_mrq(response)
            yield fundamentals
        except Exception as err:
            print('err', err)

# run: scrapy crawl profiles --nolog
