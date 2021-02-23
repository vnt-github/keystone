# BEGIN_COPYRIGHT
#
# Copyright 2009-2020 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""\
Pydoop script version of the word count example.
"""
import json
import pydoop.hdfs as hdfs
from collections import defaultdict

industries = defaultdict(dict)

with hdfs.open('/keystone/output_industries_2006_5/part-r-00000', 'rt') as f:
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

# DOCS_INCLUDE_START
def mapper(_, text, writer):
    # for word in text.split():
    data = json.loads(text)
    g_score = get_G_score(data)
    writer.emit(f"{g_score:0>2}_{data['symbol']: <10}", f"{g_score}_{data['symbol']}")

def reducer(keys, values, writer):
    g_score, symbol = next(values).split('_')
    writer.emit(symbol, g_score)

# hadoop fs -copyFromLocal /mnt/c/ms@uci/295P/keystone/tmp/results.jsonlines /keystone/input_2/results_2

# hadoop fs -cat /keystone/input_2/results_2 | less

# hadoop fs -rm -r /keystone/output_3 && pydoop script script.py /keystone/input_2 /keystone/output_3
# hadoop fs -cat /keystone/output_3/part-r-00000 | less +G





# hadoop fs -cat /keystone/output/part-r-00000 | sort -rnk 1 | less
# ORIGINAL DOCS_INCLUDE_START
# def mapper(_, text, writer):
#     # for word in text.split():
#     data = json.loads(text)
#     value = float(data['low_price']) if data['low_price'] else 0
#     writer.emit(data['symbol'], value)


# def reducer(word, icounts, writer):
#     writer.emit(sum(icounts), word)