import json, sys
from collections import defaultdict
from statistics import median
from os import walk
from math import floor, ceil

def get_G_score(data, industries):
    """
    Return g score for stocks data which signifies the stock's growth in comparison to its industry contemporaries based on fundamentals.
    
    Parameters:
        data (List[Dict]): all companies profiles data.
        industries (Dist): Industry grouped stocks median data.
    Return:
        Integer: g score
    """
    industry = data['industry']
    if not industry: return 0
    g1 = 1 if data['return_on_asset'] and (data['return_on_asset'] > industries[industry]['median_return_on_asset']) else 0
    g2 = 1 if data['cash_roa'] and (data['cash_roa'] > industries[industry]['median_cash_roa']) else 0
    g3 = 1 if data['operational_cash_flow'] and (data['operational_cash_flow'] > industries[industry]['median_operational_cash_flow']) else 0
    g4 = 1 if data['earnings_growth'] and (data['earnings_growth'] > industries[industry]['median_earnings_growth']) else 0
    g5 = 1 if data['sales_growth'] and (data['sales_growth'] > industries[industry]['median_sales_growth']) else 0
    g6 = 1 if data['capital_expenditure'] and (data['capital_expenditure'] > industries[industry]['median_capital_expenditure']) else 0
    g7 = 1 if data['rnd_expenditure'] and (data['rnd_expenditure'] > industries[industry]['median_rnd_expenditure']) else 0
    return g1 + g2 + g3 + g4 + g5 + g6 + g7

def get_F_score(data):
    """
    Return f score for stocks data which signifies the stock's growth potential based on quarterly fundamentals rate.
    
    Parameters:
        data (List[Dict]): all companies profiles data.
    Return:
        Integer: f score
    """
    f1 = 1 if (data['return_on_asset'] and data['return_on_asset'] > 0) else 0
    f2 = 1 if (data['operational_cash_flow'] and data['operational_cash_flow'] > 0) else 0
    f3 = data['delta_roa_6']
    f4 = 1 if (data['return_on_asset'] and data['operational_cash_flow'] and data['operational_cash_flow'] > data['return_on_asset']) else 0
    f5 = data['delta_leverage_6']
    f6 = data['delta_liquid_6']
    f8 = data['delta_margin_6']
    return f1 + f2 + f3 + f4 + f5 + f6 + f8

def get_trade_days(month_path):
    """
    traverse the given months director and return the first, second and last trading day of the month. 
        
    Parameters:
        month_path(String): absolute months directory path
    Return:
        Tuple(String): (first day, second day, last day)
    """
    _, str_days_and_profiles, _ = next(walk(month_path))
    str_days = sorted(day for day in str_days_and_profiles if day.isdigit())
    return (str_days[0], str_days[1], str_days[-1])

def get_trades(stocks_data, prev_day_close_path, count):
    """
    picks the best stocks for trading according the stocks data which is sorted in descending order of scores.
    trades affordable stocks volume according to the previous day's close price.
        
    Parameters:
        stocks_data (List[Dict]): all companies profiles data.
        prev_day_close_path (String): absolute close file path for the day prior to the trading day.
        count: size of portfolio
    Return:
        List(Tuple(Integer, String)): the list of tuple of trading volume and corresponding symbol.
    """
    trades = []
    close_data = [line.split() for line in open(prev_day_close_path).readlines()]
    for stock_data in stocks_data:
        if not count: break
        prev_day_trade = next((trade for trade in close_data if trade[0] == stock_data['symbol']), None)
        if not prev_day_trade: continue
        symbol, time, price, change, per_change, volume, open_p, high_p, low_p, bid, ask = prev_day_trade
        # if len(symbol) < 2: continue # 2006 10 11 uses N whose ticker does not exists and gives loss
        if low_p != 'N/A' and high_p != 'N/A': 
            price = (float(low_p) + float(high_p))/2

        each_price = floor(100000/count)
        if price == 'N/A' or not float(price): continue
        max_vol = floor(float(volume)/100)
        # trade_vol = min(max_vol, floor(each_price/float(price)))
        trade_vol = floor(each_price/float(price))
        if not trade_vol: continue
        trades.append((trade_vol, symbol))
        count -= 1
    return trades

def pick_best(stocks_data, html_format, month_1, month_2, stocks_data_dir, count):
    """
    call the stock picker based on stocks score and format for stdout.

    Parameters:
        stocks_data (List[Dict]): all companies profiles data.
        html_format (String): The html format corresponding to the year we are trading.
        month_1 (String): month prior to the trading month.
        month_2 (String): the month we are trading in.
        stocks_data_dir (String): directory path just above the stocks years directories.
    Returns:
        None: it logs the results to stdout.
    """
    time = "15:30"
    first_day, second_day, last_day = get_trade_days(f'{stocks_data_dir}/{html_format}/{month_2}')
    *_, month_1_last_day = get_trade_days(f'{stocks_data_dir}/{html_format}/{month_1}')
    trades = get_trades(stocks_data, f'{stocks_data_dir}/{html_format}/{month_1}/{month_1_last_day}/close', count)
    for volume, symbol in trades:
        print(f"{html_format}-{month_2}-{first_day} {time} buy {volume} shares of {symbol}")
        # print(f"{html_format}-{month_2}-{first_day} {time} buy {volume} shares of {symbol}")
    for volume, symbol in trades:
        print(f"{html_format}-{month_2}-{last_day} {time} sell {volume} shares of {symbol}")
    
def load_stocks_data(stocks_data_path):
    """
    loads the profiles stocks data for the trading month
    
    Parameters:
        stocks_data_path (String): full path where scraped profiles .jsonlines are saved
    Return:
        List(Dict): loads each profile .jsonline into the final data list as a dict.
    """
    data = []
    with open(stocks_data_path, 'rt') as f:
        line = f.readline()
        while line:
            data.append(json.loads(line))
            line = f.readline()
    return data

def load_industry_data(stocks_data):
    """
    calculate the industry group median values for parameters used in G-score.
    
    Parameters:
        stocks_data (List[Dict]): all companies profiles data.
    Return:
        Dist: Industry grouped stocks median data.
    """
    industries = {}
    for data in stocks_data:
        stock_industry = data['industry']
        if stock_industry not in industries:
            industries[stock_industry] = {
                'return_on_assets': [],
                'cash_roas': [],
                'operational_cash_flows': [],
                'net_incomes': [],
                'earnings_growths': [],
                'sales_growths': [],
                'capital_expenditures': [],
                'rnd_expenditures': [],
            }
        industry = industries[stock_industry]
        if data['return_on_asset'] is not None: industry['return_on_assets'].append(data['return_on_asset'])
        if data['cash_roa'] is not None: industry['cash_roas'].append(data['cash_roa'])
        if data['operational_cash_flow'] is not None: industry['operational_cash_flows'].append(data['operational_cash_flow'])
        if data['net_income'] is not None: industry['net_incomes'].append(data['net_income'])
        if data['earnings_growth'] is not None: industry['earnings_growths'].append(data['earnings_growth'])
        if data['sales_growth'] is not None: industry['sales_growths'].append(data['sales_growth'])
        if data['capital_expenditure'] is not None: industry['capital_expenditures'].append(data['capital_expenditure'])
        if data['rnd_expenditure'] is not None: industry['rnd_expenditures'].append(data['rnd_expenditure'])
    
    for industry_name in industries:
        industry = industries[industry_name]
        industry['median_return_on_asset'] = median(industry['return_on_assets']) if industry['return_on_assets'] else 0
        industry['median_cash_roa'] = median(industry['cash_roas']) if industry['cash_roas'] else 0
        industry['median_operational_cash_flow'] = median(industry['operational_cash_flows']) if industry['operational_cash_flows'] else 0
        industry['median_net_income'] = median(industry['net_incomes']) if industry['net_incomes'] else 0
        industry['median_earnings_growth'] = median(industry['earnings_growths']) if industry['earnings_growths'] else 0
        industry['median_sales_growth'] = median(industry['sales_growths']) if industry['sales_growths'] else 0
        industry['median_capital_expenditure'] = median(industry['capital_expenditures']) if industry['capital_expenditures'] else 0
        industry['median_rnd_expenditure'] = median(industry['rnd_expenditures']) if industry['rnd_expenditures'] else 0
    return industries

def set_final_score(stocks_data, industry_data):
    """
    associate scores composed of F-Score, G-Score and magic score to the stocks data.
    
    Parameters:
        stocks_data (List[Dict]): all companies profiles data.
        industry_data (Dist): Industry grouped stocks median data.
    Returns:
        None: it populates the scores field for each of stock profile.
    """
    for stock_data in stocks_data:
        # if stock_data['symbol'] in ('TEX'):
        #     print(stock_data)
        f_score = get_F_score(stock_data)
        g_score = get_G_score(stock_data, industry_data)
        magic_score = stock_data["magic_score"] if -1 <  stock_data["magic_score"] < 1 else 0 

        if stock_data['book_value_mrq'] and stock_data['price']:
            stock_data['bm_ratio'] = stock_data["book_value_mrq"]/stock_data["price"]
        else:
            stock_data['bm_ratio'] = None
        stock_data['score'] = (f_score+g_score+magic_score)
        # print(stock_data['symbol'], stock_data['score'])

def log_final_res(html_format, month_1, month_2, tmp_dir, stocks_data_dir, count=45):
    """
    main driver function which calls other functions to log the final tradings to stdout.
    
    Parameters:
        html_format (String): The html format corresponding to the year we are trading.
        month_1 (String): month prior to the trading month.
        month_2 (String): the month we are trading in.
        tmp_dir (String): the only writable directory path.
        stocks_data_dir (String): directory path just above the stocks years directories.
    Returns:
        None: it logs the results to stdout.
    """
    count = int(count)
    stocks_data_path = f"{tmp_dir}/stocks_data_{html_format}-{month_2}.jsonlines"
    stocks_data = load_stocks_data(stocks_data_path)
    industry_data = load_industry_data(stocks_data)
    set_final_score(stocks_data, industry_data)

    stocks_data = sorted(filter(lambda stock_data: stock_data['bm_ratio'], stocks_data), key=lambda stock_data: stock_data['bm_ratio'])
    ninety_percentile_size = ceil(0.1*len(stocks_data))
    stocks_size = min(ninety_percentile_size, count*4)
    stocks_data = stocks_data[:stocks_size]

    stocks_data.sort(key=lambda stock_data: stock_data['score'], reverse=True)
    
    pick_best(stocks_data, html_format, month_1, month_2, stocks_data_dir, count)

if __name__ == "__main__":
    log_final_res(*sys.argv[1:])