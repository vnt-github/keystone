import json, sys
from collections import defaultdict
from statistics import median

def get_G_score(data, industries):
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
    f1 = 1 if (data['return_on_asset'] and data['return_on_asset'] > 0) else 0
    f2 = 1 if (data['operational_cash_flow'] and data['operational_cash_flow'] > 0) else 0
    f3 = data['delta_roa']
    f4 = 1 if (data['return_on_asset'] and data['operational_cash_flow'] and data['operational_cash_flow'] > data['return_on_asset']) else 0
    f5 = data['delta_leverage']
    f6 = data['delta_liquid']
    f8 = data['delta_margin']
    return f1 + f2 + f3 + f4 + f5 + f6 + f8

def pick_best(stocks_data):
    volume = 150
    count = 20
    trades = []
    for stock_data in stocks_data:
        if stock_data['score'] > 15 or count < 0: break
        # print(stock_data['symbol'], stock_data['score'])
        trades.append((volume, stock_data['symbol'], stock_data['score']))
        count -= 1

    for volume, symbol, score in trades:
        print(f"01 15:30 buy {volume} shares of {symbol}")
    for volume, symbol, score in trades:
        print(f"30 15:30 sell {volume} shares of {symbol}")
    
def load_stocks_data(stocks_data_path):
    data = []
    with open(stocks_data_path, 'rt') as f:
        line = f.readline()
        while line:
            data.append(json.loads(line))
            line = f.readline()
    return data

def load_industry_data(stocks_data):
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
    for stock_data in stocks_data:
        f_score = get_F_score(stock_data)
        g_score = get_G_score(stock_data, industry_data)
        magic_score =stock_data["magic_score"] if -1 <  stock_data["magic_score"] < 1 else 0 
        max_value = 15
        stock_data['score'] = max_value-(f_score+g_score+magic_score)

def log_final_res(html_format, tmp_dir):
    stocks_data_path = f"{tmp_dir}/stocks_data_{html_format}.jsonlines"
    stocks_data = load_stocks_data(stocks_data_path)
    industry_data = load_industry_data(stocks_data)
    set_final_score(stocks_data, industry_data)
    stocks_data.sort(key=lambda stock_data: stock_data['score'])
    pick_best(stocks_data)

if __name__ == "__main__":
    log_final_res(sys.argv[1], sys.argv[2])