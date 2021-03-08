import json
from collections import defaultdict
import pydoop.hdfs as hdfs
# NOTE: below gives No module named 'f_score_script'
# from f_score_script import get_F_score
# from g_score_script import get_G_score


industries = defaultdict(dict)

with open('/home/vbharot/keystone/tmp/final_results/fg_industry.txt', 'rt') as f:
    line = f.readline()
    while line:
        industry, count, median_return_on_asset, median_cash_roa, median_operational_cash_flow, median_net_income, median_earnings_growth, median_sales_growth, median_capital_expenditure, median_rnd_expenditure = line.split("\t")
        if not industry: continue
        industries[industry]['count'] = float(count)
        industries[industry]['median_return_on_asset'] = float(median_return_on_asset)
        industries[industry]['median_cash_roa'] = float(median_cash_roa)
        industries[industry]['median_operational_cash_flow'] = float(median_operational_cash_flow)
        industries[industry]['median_net_income'] = float(median_net_income)
        industries[industry]['median_earnings_growth'] = float(median_earnings_growth)
        industries[industry]['median_sales_growth'] = float(median_sales_growth)
        industries[industry]['median_capital_expenditure'] = float(median_capital_expenditure)
        industries[industry]['median_rnd_expenditure'] = float(median_rnd_expenditure)
        line = f.readline()

def get_G_score(data):
    industry = data['industry']
    if not industry: return 0
    # g1 = 1 if data['return_on_asset'] and (data['return_on_asset'] > -5.787919272313815) else 0
    # g3 = 1 if data['cash_mrq'] and data['earnings_mrq'] and (data['cash_mrq'] > data['earnings_mrq']) else 0
    # g4 = 1 if data['earnings_growth'] and data['earnings_growth_industry'] and (data['earnings_growth'] > data['earnings_growth_industry']) else 0
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

def pick_best(values):
    volume = 150
    count = 20
    trades = []
    for score, symbol in values:
        if score > 15 or count < 0: break
        trades.append((volume, symbol))
        count -= 1

    for volume, symbol in trades:
        print(f"01 15:30 buy {volume} shares of {symbol}")
    for volume, symbol in trades:
        print(f"30 15:30 sell {volume} shares of {symbol}")
    

def log_final_res():
    values = []
    with open('/home/vbharot/keystone/tmp/results_fg_magic_score_2006.jsonlines', 'rt') as f:
        line = f.readline()
        while line:
            data = json.loads(line)
            f_score = get_F_score(data)
            g_score = get_G_score(data)
            max_value = 15
            magic_score = data["magic_score"] if -1 <  data["magic_score"] < 1 else 0 
            score = max_value-(f_score+g_score+magic_score)
            values.append((score, data['symbol']))
            line = f.readline()
    values.sort()
    pick_best(values)

def mapper(_, text, writer):
    # for word in text.split():
    data = json.loads(text)
    f_score = get_F_score(data)
    g_score = get_G_score(data)
    max_value = 15
    magic_score = data["magic_score"] if -1 <  data["magic_score"] < 1 else 0 
    value = f"{max_value-(f_score+g_score+magic_score):.3f}"
    writer.emit(f"{value:0>7}_{data['symbol']: <10}", f"{f_score}_{g_score}_{data['magic_score']}_{data['symbol']}")

def reducer(keys, values, writer):
    fg_score = keys.split("_")[0]
    f_score, g_score, magic_score, symbol = next(values).split('_')
    writer.emit(symbol, f"{fg_score}\t{f_score}\t{g_score}\t{magic_score}")

if __name__ == "__main__":
    log_final_res()