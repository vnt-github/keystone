import json
from statistics import median

def mapper(_, text, writer):
    data = json.loads(text)
    return_on_asset = str(data['return_on_asset'])
    cash_roa = str(data["cash_roa"])
    operational_cash_flow = str(data["operational_cash_flow"])
    net_income = str(data["net_income"])
    earnings_growth = str(data["earnings_growth"])
    sales_growth = str(data["sales_growth"])
    capital_expenditure = str(data["capital_expenditure"])
    rnd_expenditure = str(data["rnd_expenditure"])
    writer.emit(data['industry'], return_on_asset + "_" + cash_roa + "_" + operational_cash_flow + "_" + net_income + "_" + earnings_growth + "_" + sales_growth + "_" + capital_expenditure + "_" + rnd_expenditure)
    # writer.emit(data['industry'], data['return_on_asset'])

def reducer(industry, values, writer):
    count = 0
    return_on_assets = []
    cash_roas = []
    operational_cash_flows = []
    net_incomes = []
    earnings_growths = []
    sales_growths = []
    capital_expenditures = []
    rnd_expenditures = []

    for value in values:
        count += 1
        return_on_asset, cash_roa, operational_cash_flow, net_income, earnings_growth, sales_growth, capital_expenditure, rnd_expenditure = value.split('_')
        if return_on_asset != 'None':
            return_on_assets.append(float(return_on_asset))
        if cash_roa != 'None':
            cash_roas.append(float(cash_roa))
        if operational_cash_flow != 'None':
            operational_cash_flows.append(float(operational_cash_flow))
        if net_income != 'None':
            net_incomes.append(float(net_income))
        if earnings_growth != 'None':
            earnings_growths.append(float(earnings_growth))
        if sales_growth != 'None':
            sales_growths.append(float(sales_growth))
        if capital_expenditure != 'None':
            capital_expenditures.append(float(capital_expenditure))
        if rnd_expenditure != 'None':
            rnd_expenditures.append(float(rnd_expenditure))
    
    count = str(count)
    
    # total_return_on_asset = str(sum(return_on_assets))
    # total_cash_roa = str(sum(cash_roas))
    # total_operational_cash_flow = str(sum(operational_cash_flows))
    # total_net_income = str(sum(net_incomes))
    # total_earnings_growth = str(sum(earnings_growths))
    # total_sales_growth = str(sum(sales_growths))
    # total_capital_expenditure = str(sum(capital_expenditures))
    # total_rnd_expenditure = str(sum(rnd_expenditures))

    median_return_on_asset = str(median(return_on_assets)) if return_on_assets else "0"
    median_cash_roa = str(median(cash_roas)) if cash_roas else "0"
    median_operational_cash_flow = str(median(operational_cash_flows)) if operational_cash_flows else "0"
    median_net_income = str(median(net_incomes)) if net_incomes else "0"
    median_earnings_growth = str(median(earnings_growths)) if earnings_growths else "0"
    median_sales_growth = str(median(sales_growths)) if sales_growths else "0"
    median_capital_expenditure = str(median(capital_expenditures)) if capital_expenditures else "0"
    median_rnd_expenditure = str(median(rnd_expenditures)) if rnd_expenditures else "0"

    # writer.emit(industry, count + "\t" + total_return_on_asset + "\t" + total_cash_roa + "\t" + total_operational_cash_flow + "\t" + total_net_income + "\t" + total_earnings_growth + "\t" + total_sales_growth + "\t" + total_capital_expenditure + "\t" + total_rnd_expenditure)
    # writer.emit(industry, total_return_on_asset)
    # writer.emit(industry, 1)
    writer.emit(industry, count + "\t" + median_return_on_asset + "\t" + median_cash_roa + "\t" + median_operational_cash_flow + "\t" + median_net_income + "\t" + median_earnings_growth + "\t" + median_sales_growth + "\t" + median_capital_expenditure + "\t" + median_rnd_expenditure)