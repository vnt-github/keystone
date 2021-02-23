import json

def get_F_score(data):
    f1 = 1 if (data['return_on_asset'] and data['return_on_asset'] > 0) else 0
    f2 = 1 if (data['operational_cash_flow'] and data['operational_cash_flow'] > 0) else 0
    f3 = data['delta_roa']
    f4 = 1 if (data['return_on_asset'] and data['operational_cash_flow'] and data['operational_cash_flow'] > data['return_on_asset']) else 0
    f5 = data['delta_leverage']
    f6 = data['delta_liquid']
    f8 = data['delta_margin']
    return f1 + f2 + f3 + f4 + f5 + f6 + f8

# DOCS_INCLUDE_START
def mapper(_, text, writer):
    # for word in text.split():
    data = json.loads(text)
    f_score = get_F_score(data)
    writer.emit(f"{f_score:0>2}_{data['symbol']: <10}", f"{f_score}_{data['symbol']}")

def reducer(keys, values, writer):
    f_score, symbol = next(values).split('_')
    writer.emit(symbol, f_score)
