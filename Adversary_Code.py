
import math
import sys

# Opens file specified in args which consists of lists of trade.
filename = sys.argv[-1]
f = open(filename, "r")
transactions = f.readlines()
tlist = []
for x in transactions :
    tlist.append(x.split())

# Splits transactions into buy and sell. 
tlist_buy = [z for z in tlist if z[2] == 'buy']
tlist_sell = [z for z in tlist if z[2] != 'buy']

#Opens streaming file based on the transaction datestamp.
path = tlist_buy[0][0].split('-')
f = open("{}/{}/{}/streaming.tsv".format(path[0],path[1],path[2]), "r")

streaming = f.readlines()
slist = []
for x in streaming :
    slist.append(x.split())

buy_list = []

# Function to calculate the closest streaming quote in case of missing transactions at a given timestamp.
def trans_before(transaction):
    highest_time = '00:00'
    trans = []
    record = []
    for j in range(len(slist)):
        if(transaction[-1] == slist[j][0]) and (transaction[1] > slist[j][1]) and (slist[j][1] > highest_time):
            highest_time = slist[j][1]
            trans = slist[j]
   # print("Buy Instruction : ", transaction)
   # print("Nearest Streaming Quote : ", trans)
   # print("\n")
    if not trans:
        record.append(transaction[0])
        record.append('-1')       
        record.append(transaction[-1])
        record.append('-1')
        record.append(transaction[3])
        record.append('-1')
        record.append('-1')
        record.append('-1')
    else:
        record.append(transaction[0])
        record.append(trans[1])       
        record.append(transaction[-1])
        record.append(trans[2])
        record.append(transaction[3])
        record.append(trans[5])
        record.append(trans[-2])
        record.append(trans[-1])
   # print(record)
    return record

#Populates items in buy_list with relevant data required later for calculations.
for i in range(len(tlist_buy)) :
    flag_buy = False
    buy_record = []
    for j in range(len(slist)):
        if (tlist_buy[i][-1] == slist[j][0]) and (tlist_buy[i][1] == slist[j][1]) :
          #  print("Buy Instruction : ", tlist_buy[i])
           # print("Streaming Quote : ", slist[j])
        #    print("\n")
            flag_buy = True
            buy_record.append(tlist_buy[i][0])   #Date
            buy_record.append(slist[j][1])       #Time
            buy_record.append(tlist_buy[i][-1])  #Share
            buy_record.append(slist[j][2])       #Price
            buy_record.append(tlist_buy[i][3])   #Buy Vol
            buy_record.append(slist[j][5])       #Total Vol
            buy_record.append(slist[j][-2])      #Bid Price
            buy_record.append(slist[j][-1])      #Ask Price
            break
    if(flag_buy == False):
       # print("Cannot transact :", tlist_buy[i])
        buy_record = trans_before(tlist_buy[i])
    buy_list.append(buy_record)

#Function to calculate beta and DCV for stocks with missing bid ask quotes.
def calc_bidask(stock):
    vol = 0
    trans = []
    for j in range(len(slist)):
        if(stock == slist[j][0]) and (vol < int(slist[j][5])):
            vol = int(slist[j][5])
            trans = slist[j]
    if(trans):
        if (trans[7] != 'N/A') and (trans[8] != 'N/A'):
            price = round((float(trans[7]) + float(trans[8]))/2,2)
        else:
            price = round(float(trans[2]),2)
        dcv = vol * price
        #print(dcv)
        beta = round(10 ** (math.log10(dcv)/ -5),2)
        #print(beta)
        return beta, dcv
    else:
        return 0,0

for entry in buy_list:
    get_beta,dcv = calc_bidask(entry[2])
    if entry[-1] == 'N/A':
        entry[-2] = str(round(float(entry[3]) - get_beta,2))
        entry[-1] = str(round(float(entry[3]) + get_beta,2))
    entry.append(str(round(dcv,2)))
    if (float(entry[3]) * float(entry[4])) > 0.01 * dcv :
        entry.append('1')
    else:
        entry.append('0')

#Function to calculate exact buy/sell price for transcations with higher than 1% DCV transactions.
def correct_price(trans,op):           #op = 1 buy, 0 sell 
    ask = float(trans[7])
    bid = float(trans[6])
    spread = round(ask - bid,2)/2
   # print(spread)
    amt = round(float(trans[3]) * float(trans[4]),2)
  #  print(amt)
    stepSize = round(float(trans[-2])*0.01,2)
   # print(stepSize)
    numSteps = int(amt/stepSize)+1
   # print(numSteps)
    totVol = 0
    if(op == 1):
        for x in range(numSteps-1):
            totVol = totVol + stepSize / (ask + x*spread)
        remVol = float(trans[4]) - totVol
        totPrice = stepSize * (numSteps - 1) + remVol * (ask + spread*(numSteps-1))
    else:
        for x in range(numSteps-1):
            totVol = totVol + stepSize / (bid - x*spread)
        remVol = float(trans[4]) - totVol
        totPrice = stepSize * (numSteps - 1) + remVol * (bid - spread*(numSteps-1))
    avgPrice = round(totPrice / float(trans[4]),2)
 #   print(avgPrice)
    return avgPrice,(numSteps-1)

total_asset = 100000

#Function to perform buy operations maintaining all constraints.
def execute_buy(buy_list, total_asset):
    counter = 1
    act_balance = total_asset
    stock_list = dict()
    for item in buy_list:
        if item[1] == '-1':
            print("##### ERROR : STOCK {}, DID NOT TRADE ON SELECTED DAY. #####".format(item[2]))
        else:
            if item[-1] == '1':
                price,percent = correct_price(entry,1)
                print("#####      WARNING : Will require atleast {}% of DCV".format(percent))
                entry[7] = price

            tot_cost = 0
            cost = float(item[4]) * float(item[7])
            if counter > 10:
                tot_cost = round(cost + 1,2)
                act_balance = round(act_balance - tot_cost,2) 
            if counter < 10:
                tot_cost = round(cost + 10,2)
                act_balance = round(act_balance - tot_cost,2)
            if counter == 10:
                tot_cost = round(cost + 10,2)
                act_balance = round(act_balance - tot_cost + 89,2)
            if act_balance >= 0:
                total_asset = act_balance
                print("{} {} {} {} bought at ${}; Cash Spent ${}; Cash Balance ${}".format(item[0],item[1],item[4],item[2],item[7],tot_cost,act_balance))
                counter = counter + 1
                new_entry = []
                new_entry.append(item[2])
                new_entry.append(item[4])
                val = stock_list.get(item[2],'0')
                stock_list[item[2]] = str(float(val)+float(item[4]))
            else:
                print("{} {} purchase requires cash ${}\n##### ERROR : NOT ENOUGH BALANCE TO PROCEED WITH THIS TRANSACTION. #####".format(item[4],item[2],tot_cost))
    return total_asset, stock_list

#Updates the assets remaining after executing all operations in the buy list and the corresponding holdings.
total_asset, holdings = execute_buy(buy_list, total_asset)

path = tlist_sell[0][0].split('-')

#Reopens the streaming file for sell transactions.
f = open("{}/{}/{}/streaming.tsv".format(path[0],path[1],path[2]), "r")
streaming = f.readlines()
slist = []
for x in streaming :
    slist.append(x.split())

sell_list = []

for i in range(len(tlist_sell)) :
    flag_sell = False
    sell_record = []
    for j in range(len(slist)):
        if (tlist_sell[i][-1] == slist[j][0]) and (tlist_sell[i][1] == slist[j][1]) :
          #  print("Buy Instruction : ", tlist_buy[i])
           # print("Streaming Quote : ", slist[j])
        #    print("\n")
            flag_sell = True
            sell_record.append(tlist_sell[i][0])   #Date
            sell_record.append(slist[j][1])       #Time
            sell_record.append(tlist_sell[i][-1])  #Share
            sell_record.append(slist[j][2])       #Price
            sell_record.append(tlist_sell[i][3])   #Buy Vol
            sell_record.append(slist[j][5])       #Total Vol
            sell_record.append(slist[j][-2])      #Bid Price
            sell_record.append(slist[j][-1])      #Ask Price
            break
    if(flag_sell == False):
       # print("Cannot transact :", tlist_sell[i])
        sell_record = trans_before(tlist_sell[i])
    sell_list.append(sell_record)

 #For quotes with missing bid/ask, gets those values from calculating beta and DCV.
for entry in sell_list:
    get_beta,dcv = calc_bidask(entry[2])
    if entry[-1] == 'N/A':
        entry[-2] = str(round(float(entry[3]) - get_beta,2))
        entry[-1] = str(round(float(entry[3]) + get_beta,2))
    entry.append(str(round(dcv,2)))
    if (float(entry[3]) * float(entry[4])) > 0.01 * dcv :
        entry.append('1')
    else:
        entry.append('0')

#Function to perform sell operations with all contraints maintained.
def execute_sell(sell_list, total_asset):
    counter = 1
    act_balance = total_asset
    for item in sell_list:
        if item[1] == '-1':
            print("##### ERROR : STOCK {}, DID NOT TRADE ON SELECTED DAY. #####; Cash Acquired $0; Cash Balance ${}".format(item[2],act_balance))
            if(float(holdings.get(item[2],-5)) != -5):
                holdings[item[2]] = '0.0'
        else:
            if (item[2] in holdings) and (float(item[4]) <= float(holdings[item[2]])):
                if item[-1] == '1':
                    price,percent = correct_price(entry,0)
                    print("#####        WARNING : Will require atleast {}% of DCV".format(percent))
                    entry[6] = price

                tot_cost = 0
                cost = float(item[4]) * float(item[7])
                if counter > 10:
                    tot_cost = round(cost - 1,2)
                    act_balance = round(act_balance + tot_cost,2) 
                if counter < 10:
                    tot_cost = round(cost - 10,2)
                    act_balance = round(act_balance + tot_cost,2)
                if counter == 10:
                    tot_cost = round(cost - 10,2)
                    act_balance = round(act_balance + tot_cost + 89,2)
                total_asset = act_balance
                print("{} {} {} {} sold at ${}; Cash Acquired ${}; Cash Balance ${}".format(item[0],item[1],item[4],item[2], item[6],tot_cost,act_balance))
                counter = counter + 1
                sharesLeft = float(holdings.get(item[2])) - float(item[4])
                holdings[item[2]] = str(sharesLeft)
            else:
                print("##### ERROR : Not enough holdings of {} to sell. #####".format(item[2]))
    return total_asset

#Updates total assets after performing buy operations.
total_asset = execute_sell(sell_list, total_asset)

#Prints the final balance after all transactions.
print("\nAccount Status : Cash Balance is : ${}\n".format(total_asset))

