from os import readlink
import sys
import pydoop.hdfs as hdfs

def pick_best(html_format):
    with hdfs.open(f"/pydoop_out/fg_{html_format}/part-r-00000", "rt") as f:
        line = f.readline()
        i = 0
        sells = []
        volume = 150
        while line:
            values = line.split('\t')
            symbol = values[0]
            score = float(values[1])
            if score <= 0:
                print("invalid", symbol, score)
            if score > 14 or i > 20:
                break
            i += 1
            print(f"01 15:30 buy {volume} shares of {symbol}")
            sells.append(f"30 15:30 sell {volume} shares of {symbol}")
            line = f.readline()
        
        for sell in sells:
            print(sell)


if __name__ == "__main__":
    pick_best(sys.argv[1])