import scrapy
import re
import traceback
from os import walk
from string import ascii_lowercase
from scraper.items import Fundamentals

class ProfilesSpider(scrapy.Spider):
    name = "profiles_2006"
    def start_requests(self):
        """
        return an iterable with the first Requests to crawl for this spider. It is called by Scrapy when the spider is opened for scraping.
        Here we traverse the stocks directory with profiles and crawl a-z directories.
        """
        path_prefix = f'{self.stocks_data_dir}/{self.html_format}/{self.month_2}/profiles/Yahoo/US/01/p/'
        for alphabet in ascii_lowercase:
            counter = 2
            for dirpath, dirnames, filenames in walk(path_prefix + alphabet):
                for filename in filenames:
                    if filename.count('.') > 1: continue
                    counter -= 1
                    if not counter and hasattr(self, 'is_testing'):
                        break
                    yield scrapy.Request(url=f'file://{path_prefix}{alphabet}/{filename}')
        # yield scrapy.Request(url=f'file://{path_prefix}g/GOOG.html')

    def get_symbol(self, response):
        """
        Return symbol associated
        """
        start_i = response.request.url.rfind('/')
        end_i = response.request.url.rfind('.')
        return response.request.url[start_i+1:end_i]

    def extract_growth_value_from_siblings(self, key_selector):
        "generaic value extractor from the adjacent field in table for given key_selector"
        value = key_selector.xpath("./following-sibling::td/text()").get()
        industry = key_selector.xpath("./following-sibling::td[5]/text()").get()
        return self.convert_str_to_number(value) if value != 'N/A' else None, self.convert_str_to_number(industry) if industry != 'N/A' else None

    @staticmethod
    def convert_str_to_number(num):
        "converts extracted string to intergers handling various suffixes, prefixes and delimiters"
        try:
            char_map = {'K':1000, 'M':1000000, 'B':1000000000}
            num = re.sub("[,%)()$]", "", num)
            if num and num[-1] in char_map.keys():
                return float(num[:-1]) * char_map[num[-1].upper()]
            else:
                return float(num)
        except Exception as err:
            # print(f'err: convert_str_to_number: {num}', err)
            return

    def extract_value_from_key_sibling(self, key_selector):
        "generaic value extractor from the adjacent field in table for given key_selector"
        try:
            #sign = -1 if key_selector.xpath("./following-sibling::td/text()").get() == '-' else 1
            value = key_selector.xpath("./following-sibling::td/text()").get()
            if not value: return
            #return sign*self.convert_str_to_number(value)
            return self.convert_str_to_number(value)
        except Exception as err:
            # print('sibling extraction err', err)
            return

    def extract_value_from_key_sibling_2(self, key_selector):
        "generaic value extractor from the adjacent field in table for given key_selector in second format"
        try:
            #sign = -1 if key_selector.xpath("./following-sibling::td/text()").get() == '-' else 1
            value = key_selector.xpath("./following-sibling::td:nth-child(5)/text()").get()
            if not value: return
            #return sign*self.convert_str_to_number(value)
            return self.convert_str_to_number(value)
        except Exception as err:
            # print('sibling extraction err', err)
            return

    def extract_low_price(self, response):
        """
        Return extracted low price by from profile
        """
        low_price_selectors = response.xpath("//*[contains(text(), '52-Week Low')]")
        if not low_price_selectors:
            # print(f'symbol: {self.symbol} missing 52-Week Low')
            return
        return self.extract_value_from_key_sibling(low_price_selectors[0])

    def extract_high_price(self, response):
        """
        Return extracted high price by from profile
        """
        high_price_selectors = response.xpath("//*[contains(text(), '52-Week High')]")
        if not high_price_selectors:
            # print(f'symbol: {self.symbol} missing 52-Week High')
            return
        return self.extract_value_from_key_sibling(high_price_selectors[0])

    def extract_book_value_mrq(self, response):
        "Return extracted book value for most recent quarter from profile"
        book_value_mrq_selectors = response.xpath("//*[contains(text(), 'Book Value')]")
        if not book_value_mrq_selectors:
            # print(f'symbol: {self.symbol} missing Book Value (mrq)')
            return
        return self.extract_value_from_key_sibling(book_value_mrq_selectors[0])

    def extract_earnings_ttm(self, response):
        "Return extracted earnings value for trailing twelve months"
        earnings_ttm_selectors = response.xpath("//*[contains(text(), 'Earnings')]/*[contains(text(), '(ttm)')]/..")
        if not earnings_ttm_selectors:
            # print(f'symbol: {self.symbol} missing Earnings (ttm)')
            return

        return self.extract_value_from_key_sibling(earnings_ttm_selectors[0])
   
    def extract_earnings_mrq(self, response):
        "Return extracted earnings for most recent quarter from profile"
        earnings_mrq_selectors = response.xpath("//*[contains(text(), 'EPS Est')]")
        if not earnings_mrq_selectors:
            # print(f'symbol: {self.symbol} missing Earnings (mrq)')
            return

        return self.extract_value_from_key_sibling_2(earnings_mrq_selectors[0])

    def extract_daily_volume(self, response):
        "Return extracted daily volume from profile"
        daily_volume_selectors = response.xpath("//*[contains(text(), 'Average Volume')]")
        if not daily_volume_selectors:
            # print(f'symbol: {self.symbol} missing Daily Volume (3-month avg)')
            return
        return self.extract_value_from_key_sibling(daily_volume_selectors[0])


    def extract_shares_outstanding(self, response):
        "Return extracted shares outstanding from profile"
        shares_outstanding_selectors = response.xpath("//*[contains(text(), 'Shares Outstanding')]")
        if not shares_outstanding_selectors:
            # print(f'symbol: {self.symbol} missing Shares Outstanding')
            return
        return self.extract_value_from_key_sibling(shares_outstanding_selectors[0])

    def extract_return_on_asset(self, response):
        "Return extracted retur on asset from profile"
        return_on_assets_selectors = response.xpath("//*[contains(text(), 'Return on Assets (ttm)')]")
        if not return_on_assets_selectors:
            # print(f'symbol: {self.symbol} missing return on asset')
            return
        return self.extract_value_from_key_sibling(return_on_assets_selectors)

    def extract_industry(self, response):
        "Extract and Return industry for stock"
        industry_selectors = response.xpath("//*[contains(text(), 'Industry:')]")
        if not industry_selectors:
            # print(f'symbol: {self.symbol} missing Industry:')
            return
        return industry_selectors.xpath("../td[2]/a/text()").get()

    def extract_operational_cash_flow(self, response):
        "Extract and Return operational cash flow"
        operational_cash_flow_selectors = response.xpath("//*[contains(text(), 'Operating Cash Flow (ttm):')]")
        if not operational_cash_flow_selectors:
            # print(f'symbol: {self.symbol} missing Operating Cash Flow (ttm):')
            return
        return self.extract_value_from_key_sibling(operational_cash_flow_selectors)

    def extract_total_assets(self, response):
        "Extract and Return total assets"
        total_assets_selectors = response.xpath("//*[contains(text(), 'Total Assets')]/..")
        if not total_assets_selectors:
            # print(f'symbol: {self.symbol} missing Total Assets')
            return
        # these each quaterly value need to be sum verify the Revenue (ttm) and sum of Total Revenue
        values = list(map(self.convert_str_to_number, total_assets_selectors.xpath("../td/b/text()").getall()[1:]))
        return sum(each*1000 for each in values if each is not None)

    def extract_net_income(self, response):
        "Extract and Return net income"
        net_income_selectors = response.xpath("//*[contains(text(), 'Net Income (ttm):')]")
        if not net_income_selectors:
            # print(f'symbol: {self.symbol} missing Net Income (ttm):')
            return
        return self.extract_value_from_key_sibling(net_income_selectors)
        
    def extract_earnings_growth(self, response):
        "Extract and Return earnings % growth for G-Score"
        earnings_growth_selectors = response.xpath("//*[contains(text(), 'Qtrly Earnings Growth (yoy):')]")
        if not earnings_growth_selectors:
            # print(f'symbol: {self.symbol} missing Qtrly Earnings Growth (yoy):')
            return
        return self.extract_value_from_key_sibling(earnings_growth_selectors)
            

    def extract_sales_growth(self, response):
        "Extract and Return sales % growth for G-Score"
        sales_growth_selectors = response.xpath("//*[contains(text(), 'Sales Growth (year/est)')]")
        if not sales_growth_selectors:
            # print(f'symbol: {self.symbol} missing Sales Growth (year/est)')
            return
        return self.extract_value_from_key_sibling(sales_growth_selectors)

    def extract_revenue_growth(self, response):
        "Extract and Return revenue % growth for G-Score potential backup"
        revenue_growth_selectors = response.xpath("//*[contains(text(), 'Qtrly Rev Growth (yoy):')]")
        if not revenue_growth_selectors:
            # print(f'symbol: {self.symbol} missing Qtrly Rev Growth (yoy):')
            return (None, None)
        return self.extract_growth_value_from_siblings(revenue_growth_selectors)

    def extract_capital_expenditure(self, response):
        "Extract and Return total Capital Expenditure for G-Score"
        capital_expenditure_selectors = response.xpath("//*[contains(text(), 'Capital Expenditures')]")
        if not capital_expenditure_selectors:
            # print(f'symbol: {self.symbol} missing Capital Expenditures')
            return
        values = list(map(self.convert_str_to_number, capital_expenditure_selectors.xpath('../td/text()').getall()[1:]))
        return sum(each*1000 for each in values if each is not None)


    def extract_rnd_expenditure(self, response):
        "Extract and Return total R&D Expenditure for G-Score"
        rnd_expenditure_selectors = response.xpath("//*[contains(text(), 'Research Development')]")
        if not rnd_expenditure_selectors:
            # print(f'symbol: {self.symbol} missing Research Development')
            return
        values = list(map(self.convert_str_to_number, rnd_expenditure_selectors.xpath('../td/text()').getall()[1:]))
        return sum(each*1000 for each in values if each is not None)

    def extract_ebitda(self, response):
        "Extract and Return EBITDA value for magic score"
        ebitda_selectors = response.xpath("//*[contains(text(), 'EBITDA (ttm):')]")
        if not ebitda_selectors:
            # print(f'symbol: {self.symbol} missing EBITDA (ttm):')
            return
        return self.extract_value_from_key_sibling(ebitda_selectors[0])

    def extract_enterprise_value(self, response):
        "Extract and Return enterprise value for magic score"
        enterprise_selectors = response.xpath("//*[contains(text(), 'Enterprise Value')]")
        if not enterprise_selectors:
            # print(f'symbol: {self.symbol} missing Enterprise Value')
            return
        return self.extract_value_from_key_sibling(enterprise_selectors[0])

    def extract_quarterly_total_assets(self, response):
        "Extract and Return list with quarterly total assets for F-Score ∆ROA and ∆LEVERAGE"
        q_total_assets_selectors = response.xpath("//*[contains(text(), 'Total Assets')]")
        if not q_total_assets_selectors:
            # print(f'symbol: {self.symbol} missing Total Assets')
            return
        values = list(map(self.convert_str_to_number, q_total_assets_selectors[0].xpath("../../td/b/text()").getall()[1:]))
        return [each*1000 if each is not None else None for each in values]

    def extract_quarterly_net_income(self, response):
        "Extract and Return list with quarterly net income for F-Score ∆ROA"
        q_net_income_selectors = response.xpath("//*[contains(text(), 'Net Income Applicable To Common Shares')]")
        if not q_net_income_selectors:
            # print(f'symbol: {self.symbol} missing Net Income Applicable To Common Shares')
            return
        values = list(map(self.convert_str_to_number, q_net_income_selectors[0].xpath("../../td/b/text()").getall()[1:]))
        return [each*1000 if each is not None else None for each in values]

    def extract_quarterly_total_liabilities(self, response):
        "Extract and Return list with quarterly total liabilitis as backup for long term debt for F-Score ∆LEVERAGE"
        q_total_liabilities_selectors = response.xpath("//*[contains(text(), 'Total Liabilities')]")
        if not q_total_liabilities_selectors:
            # print(f'symbol: {self.symbol} missing Total Liabilities')
            return
        values = list(map(self.convert_str_to_number, q_total_liabilities_selectors[0].xpath("../../td/b/text()").getall()[1:]))
        return [each*1000 if each is not None else None for each in values]

    def extract_quarterly_long_term_debt(self, response):
        "Extract and Return list with quarterly long term debt for F-Score ∆LEVERAGE"
        q_long_term_debt_selectors = response.xpath("//*[contains(text(), 'Long Term Debt')]")
        if len(q_long_term_debt_selectors) < 2:
            # print(f'symbol: {self.symbol} missing Total Assets')
            return
        values = list(map(self.convert_str_to_number, q_long_term_debt_selectors[1].xpath("../td/text()").getall()[1:]))
        return [each*1000 if each is not None else None for each in values]

    def extract_quarterly_current_assets(self, response):
        "Extract and Return list with quarterly current assets for F-Score ∆LIQUID"
        q_current_assets_selectors = response.xpath("//*[contains(text(), 'Total Current Assets')]")
        if not q_current_assets_selectors:
            # print(f'symbol: {self.symbol} missing Total Current Assets')
            return
        values = list(map(self.convert_str_to_number, q_current_assets_selectors[0].xpath("../../td/b/text()").getall()[1:]))
        return [each*1000 if each is not None else None for each in values]
        
    def extract_quarterly_current_liabilities(self, response):
        "Extract and Return list with quarterly current liabilities for F-Score ∆LIQUID"
        q_current_liabilities_selectors = response.xpath("//*[contains(text(), 'Total Current Liabilities')]")
        if not q_current_liabilities_selectors:
            # print(f'symbol: {self.symbol} missing Total Current Liabilities')
            return
        values = list(map(self.convert_str_to_number, q_current_liabilities_selectors[0].xpath("../../td/b/text()").getall()[1:]))
        return [each*1000 if each is not None else None for each in values]
        
    def extract_quarterly_gross_profit(self, response):
        "Extract and Return list with quarterly gross profits for F-Score ∆MARGIN"
        q_gross_profit_selectors = response.xpath("//*[contains(text(), 'Gross Profit')]")
        if len(q_gross_profit_selectors) < 2:
            # print(f'symbol: {self.symbol} missing Gross Profit')
            return
        values = list(map(self.convert_str_to_number, q_gross_profit_selectors[1].xpath("../../td/b/text()").getall()[1:]))
        return [each*1000 if each is not None else None for each in values]
        
    def extract_quarterly_total_revenue(self, response):
        "Extract and Return List with quarterly total revenue for F-Score ∆MARGIN"
        q_total_revenue_selectors = response.xpath("//*[contains(text(), 'Total Revenue')]")
        if not q_total_revenue_selectors:
            # print(f'symbol: {self.symbol} missing Total Revenue')
            return
        values = list(map(self.convert_str_to_number, q_total_revenue_selectors[0].xpath("../../td/b/text()").getall()[1:]))
        return [each*1000 if each is not None else None for each in values]

    def parse(self, response):
        """
        Crawler callbacks parse function for each scrapped URL with the response, which we used to extract relavant information.
        The scraped information is stored in Fundamentals object Item, which is stored as .jsonline in the final output file.
        
        Parameters:
            response (Response) – the response to parse
        Return:
            Object: Fundamentals item object
        """
        try:
            fundamentals = Fundamentals()
            fundamentals['symbol'] = self.get_symbol(response)
            self.symbol = fundamentals['symbol']
            fundamentals['low_price'] = self.extract_low_price(response)
            fundamentals['high_price'] = self.extract_high_price(response)
            fundamentals['shares_outstanding'] = self.extract_shares_outstanding(response)

            fundamentals['daily_volume'] = self.extract_daily_volume(response)
            
            fundamentals['book_value_mrq'] = self.extract_book_value_mrq(response)
            
         #   fundamentals['earnings_ttm'] = self.extract_earnings_ttm(response)
         #   fundamentals['earnings_mrq'] = self.extract_earnings_mrq(response)
         
            #for magic
            fundamentals['ebitda'] = self.extract_ebitda(response)
            fundamentals['enterprise_value'] = self.extract_enterprise_value(response)

            # for g score
            fundamentals['return_on_asset'] = self.extract_return_on_asset(response)
            fundamentals['industry'] = self.extract_industry(response)
            fundamentals['operational_cash_flow'] = self.extract_operational_cash_flow(response)
            fundamentals['total_assets'] = self.extract_total_assets(response)
            fundamentals['net_income'] = self.extract_net_income(response)
            fundamentals['earnings_growth'] = self.extract_earnings_growth(response)
            fundamentals['sales_growth'] = self.extract_sales_growth(response)
            fundamentals['revenue_growth'], fundamentals['revenue_growth_industry'] = self.extract_revenue_growth(response)
            fundamentals['capital_expenditure'] = self.extract_capital_expenditure(response) 
            fundamentals['rnd_expenditure'] = self.extract_rnd_expenditure(response)
            
            # for f score
            fundamentals['quarterly_net_income'] = self.extract_quarterly_net_income(response)
            fundamentals['quarterly_total_assets'] = self.extract_quarterly_total_assets(response)
            fundamentals['quarterly_long_term_debt'] = self.extract_quarterly_long_term_debt(response)
            fundamentals['quarterly_current_assets'] = self.extract_quarterly_current_assets(response)
            fundamentals['quarterly_current_liabilities'] = self.extract_quarterly_current_liabilities(response)
            fundamentals['quarterly_gross_profit'] = self.extract_quarterly_gross_profit(response)
            fundamentals['quarterly_total_revenue'] = self.extract_quarterly_total_revenue(response)
            fundamentals['quarterly_total_liabilities'] = self.extract_quarterly_total_liabilities(response)
            yield fundamentals
        except Exception as err:
            traceback.print_exc()
            print(f'ERROR omission for {self.symbol}', err)

# run: scrapy crawl profiles --nolog
