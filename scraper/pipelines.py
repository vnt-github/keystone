import numpy as np

class FundamentalsPipeline(object):
    def normalize_by_price(self, value, price):
        "normalize given value by stock price"
        if not value or not price: return
        return value/price

    def normal_distribution(self, high, low):
        "pick normally distributed price value between high and low"
        if not high or not low: return
        mean, standard_deviation = (high+low)/2, 0 # mean and standard deviation
        # NOTE: we want consistent price value to compare different results wrt different bm_ratio, which is consistent in between runs hence 0 deviation
        return np.random.normal(mean, standard_deviation)

    def get_cash_roa(self, operational_cash_flow, total_assets):
        "calcuate and return cash return on asset"
        if not operational_cash_flow or not total_assets: return
        return operational_cash_flow/total_assets*100

    def get_delta_ratio(self, a, b, sign=1):
        "generic function to calculate quarterly delta value change for a/b ratio"
        deltas = 0
        if not a or not b or not all(a) or not all(b) or len(a) < 4 or len(b) < 4 or len(a) != len(b): return 0
        for i in range(len(a)-1):
            if not a[i] or not b[i] or not a[i+1] or not b[i+1]: continue
            curr_year_roa = a[i]/b[i]
            prev_year_roa = a[i+1]/b[i+1]
            # each quarterly increment/decrement is normalized to +1 or -1 because we want to see the trend in the change not the amount of change
            deltas += (sign if curr_year_roa > prev_year_roa else -sign)
        return int(deltas > 0)

    def get_delta_ratio_6(self, a, b, should_increase=1):
        "generic function to calculate half yearly delta value change for a/b ratio"
        should_increase = (should_increase == 1)
        if not a or not b or not all(a) or not all(b) or len(a) < 4 or len(b) < 4 or not sum(b[2:]) or not sum(b[:2]): return 0
        has_increased = sum(a[:2])/sum(b[:2]) > sum(a[2:])/sum(b[2:])
        return 1 if ((has_increased and should_increase) or (not has_increased and not should_increase)) else 0

    def get_delta_roa(self, quarterly_net_income, quarterly_total_assets):
        " calculating delta roa here for f score because we don't need any aggregation from other data"
        return self.get_delta_ratio(quarterly_net_income, quarterly_total_assets), self.get_delta_ratio_6(quarterly_net_income, quarterly_total_assets)

    def get_delta_leverage(self, quarterly_long_term_debt, quarterly_total_assets):
        "calculating delta leverage here for f score because we don't need any aggregation from other data"
        return self.get_delta_ratio(quarterly_long_term_debt, quarterly_total_assets, -1), self.get_delta_ratio_6(quarterly_long_term_debt, quarterly_total_assets, -1)

    def get_delta_liquid(self, quarterly_current_assets, quarterly_current_liabilities):
        "calculating delta roa here for f score because we don't need any aggregation from other data"
        return self.get_delta_ratio(quarterly_current_assets, quarterly_current_liabilities), self.get_delta_ratio_6(quarterly_current_assets, quarterly_current_liabilities)

    def get_delta_margin(self, quarterly_gross_profit, quarterly_total_revenue):
        "calculating delta margin here for f score because we don't need any aggregation from other data"
        return self.get_delta_ratio(quarterly_gross_profit, quarterly_total_revenue), self.get_delta_ratio_6(quarterly_gross_profit, quarterly_total_revenue)

    def get_magic_score(self, ebitda, enterprise_value):
        if not ebitda or not enterprise_value: return 0
        return ebitda/enterprise_value

    def process_item(self, item, spider):
        """
        This method is called for every item pipeline component.
        We use this to populate fundamental values which are derived from other fundamentals value.
            Parameters:
                item (item object) – the scraped item
                spider (Spider object) – the spider which scraped the item
            Return:
                Object: item with additional fields
        """
        item['price'] = self.normal_distribution(item['high_price'], item['low_price'])
        # item['earnings_ttm_per_price'] = self.normalize_by_price(item['earnings_ttm'], item['price'])
        # item['earnings_mrq_per_price'] = self.normalize_by_price(item['earnings_mrq'], item['price'])
        item['cash_roa'] = self.get_cash_roa(item['operational_cash_flow'], item['total_assets'])

        item['delta_roa'], item['delta_roa_6'] = self.get_delta_roa(item['quarterly_net_income'], item['quarterly_total_assets'])
        if item['quarterly_long_term_debt'] and all(item['quarterly_long_term_debt']):
            item['delta_leverage'], item['delta_leverage_6'] = self.get_delta_leverage(item['quarterly_long_term_debt'], item['quarterly_total_assets'])
        else:
            item['delta_leverage'], item['delta_leverage_6'] = self.get_delta_leverage(item['quarterly_total_liabilities'], item['quarterly_total_assets'])
        item['delta_liquid'], item['delta_liquid_6'] = self.get_delta_liquid(item['quarterly_current_assets'], item['quarterly_current_liabilities'])
        item['delta_margin'], item['delta_margin_6'] = self.get_delta_margin(item['quarterly_gross_profit'], item['quarterly_total_revenue'])
        item['magic_score']= self.get_magic_score(item['ebitda'], item['enterprise_value'])
        return item