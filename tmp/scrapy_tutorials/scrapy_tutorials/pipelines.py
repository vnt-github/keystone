import numpy as np

class FundamentalsPipeline(object):
    def normalize_by_price(self, value, price):
        if not value or not price: return
        return value/price

    def normal_distribution(self, high, low):
        if not high or not low: return
        mean, standard_deviation = (high+low)/2, 0.1 # mean and standard deviation
        return np.random.normal(mean, standard_deviation)

    def process_item(self, item, spider):
        item['price'] = self.normal_distribution(item['high_price'], item['low_price'])
        item['earnings_ttm_per_price'] = self.normalize_by_price(item['earnings_ttm'], item['price'])
        item['earnings_mrq_per_price'] = self.normalize_by_price(item['earnings_mrq'], item['price'])
        return item