from scrapy.item import Item, Field

class Fundamentals(Item):
    symbol = Field()
    low_price = Field()
    high_price = Field()
    price = Field()
    shares_outstanding = Field()
    daily_volume = Field()
    book_value_mrq = Field()
    # earnings_ttm = Field(default=0)
    # earnings_mrq = Field(default=0)
    # earnings_ttm_per_price = Field()
    # earnings_mrq_per_price = Field()
    return_on_asset = Field()
    total_cash = Field()
    earnings_growth = Field()
    earnings_growth_industry = Field()
    cash_mrq = Field()
    cash_roa = Field()
    industry = Field()
    operational_cash_flow = Field()
    total_assets = Field()
    net_income = Field()
    sales_growth = Field()
    revenue_growth = Field()
    revenue_growth_industry = Field()
    capital_expenditure = Field()
    rnd_expenditure = Field()

    

