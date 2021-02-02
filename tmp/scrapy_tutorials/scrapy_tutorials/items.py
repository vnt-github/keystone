from scrapy.item import Item, Field

class Fundamentals(Item):
    symbol = Field()
    low_price = Field()
    high_price = Field()
    price = Field()
    shares_outstanding = Field()
    daily_volume = Field()
    book_value_mrq = Field()
    earnings_ttm = Field()
    earnings_mrq = Field()
    earnings_ttm_per_price = Field()
    earnings_mrq_per_price = Field()
    

