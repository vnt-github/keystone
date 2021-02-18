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

def get_G_score(data):
    g1 = 1 if data['return_on_asset'] and (data['return_on_asset'] > -5.787919272313815) else 0
    g3 = 1 if data['cash_mrq'] and data['earnings_mrq'] and (data['cash_mrq'] > data['earnings_mrq']) else 0
    g4 = 1 if data['earnings_growth'] and data['earnings_growth_industry'] and (data['earnings_growth'] > data['earnings_growth_industry']) else 0
    return g1 + g3 + g4

# DOCS_INCLUDE_START
def mapper(_, text, writer):
    # for word in text.split():
    data = json.loads(text)
    g_score = get_G_score(data)
    value = float(data['low_price']) if data['low_price'] else 0
    v1 = f'{value:.3f}'
    # writer.emit(f'{v1:0>8}', data['symbol']+"_"+str(value))
    writer.emit(f'{g_score}_{v1:0>8}', data['symbol']+"_"+str(g_score))

def reducer(value, symbols, writer):
    g_score, low_price = value.split('_')
    writer.emit(next(symbols), str(float(low_price)))

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