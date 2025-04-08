import numpy as np
import pandas as pd
from collections import defaultdict
import jsonpickle as jp
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


class Visualizer:

    class tradelog:
        def __init__(self, log):
            self._dict = log
        def __getattr__(self, w):
            return self._dict.get(w, "")

    class OrderDepth:
        def __init__(self):
                self.buy_orders = None
                self.sell_orders = None

    def __init__(self, log_file, tmp_file=r"/Users/ysc/Desktop/imcProsperity/cache/tmp_v_.csv"):
        with open(log_file) as f:
            tmp = f.read()
            start = tmp.find("Activities log:") + 16
            end = tmp.find("\n\n\n\n", start)
            tmp = tmp[start:end]
        with open(tmp_file, mode='w') as f:
            f.write(tmp)
        self.df = pd.read_csv(tmp_file, sep=";")
        self.timestamp = np.arange(0, self.df['timestamp'].max() + 100, 100)

        with open(log_file) as f:
            o = f.read()
            start = o.find("Trade History:\n")   
            o = o[start+15:]  
        self.transaction_log = [self.tradelog(l) for l in jp.decode(o)]

    @staticmethod
    def calculate_wavg_midprice(order_depth):
        sorted_buy_orders = sorted(order_depth.buy_orders.items(), reverse=True)
        sorted_sell_orders = sorted(order_depth.sell_orders.items(), reverse=False)
        total_volume = 0
        wavg_price = 0
        for p, vol in sorted_buy_orders:
                wavg_price += p * vol
                total_volume += vol
        for p, vol in sorted_sell_orders:
                vol = abs(vol)
                wavg_price += p * vol
                total_volume += vol
        wavg_price /= total_volume
        return wavg_price

    def plot_static(self, product, start_timestamp = 0, end_timestamp = 100, display_mid_price=True, display_book=True, display_pos=True, display_my_order=True, display_other_order=True):
        my_buy_hist, my_sell_hist, other_hist = defaultdict(lambda : defaultdict(float)), defaultdict(lambda : defaultdict(float)), defaultdict(lambda : defaultdict(float))
        cnt_buy = cnt_sell = 0
        for l in self.transaction_log:
            if l.symbol == product:
                if l.seller == "SUBMISSION":
                    my_sell_hist[l.timestamp // 100][l.price] += l.quantity
                    cnt_sell += 1
                elif l.buyer == "SUBMISSION":
                    my_buy_hist[l.timestamp // 100][l.price] += l.quantity
                    cnt_buy += 1
                else:
                    other_hist[l.timestamp // 100][l.price] += l.quantity
                     
        pos = [0]
        mid_price = []
        for ts in self.timestamp:
            cur, idx =0, ts / 100
            if idx in my_buy_hist:
                for _, v in my_buy_hist[idx].items():
                    cur += v
            if idx in my_sell_hist:
                for _, v in my_sell_hist[idx].items():
                    cur -= v
            pos += [pos[-1] + cur]

        df = self.df[self.df['product'] == product]
        df.reset_index(inplace=True)

        _, ax = plt.subplots(figsize=(20, 12))
        order_depth_ = self.OrderDepth()

        for _, row in df.iterrows():

            idx = row['timestamp'] // 100
            if idx < start_timestamp:
                    continue
            if idx > end_timestamp:
                    break

            idx = row['timestamp'] // 100
            dict_ = dict()
            
            for i in (1, 2, 3):
                if row[f'bid_volume_{i}'] != 0 and not np.isnan(row[f'bid_volume_{i}']):
                    if display_book:
                        ax.scatter(idx, row[f'bid_price_{i}'], c='blue', s=row[f'bid_volume_{i}'])
                    dict_[row[f'bid_price_{i}']] = row[f'bid_volume_{i}']
            order_depth_.buy_orders = dict_
            dict_ = dict()
            for i in (1, 2, 3):
                if row[f'ask_volume_{i}'] != 0 and not np.isnan(row[f'ask_volume_{i}']):
                    if display_book:
                        ax.scatter(idx, row[f'ask_price_{i}'], c='red', s=row[f'ask_volume_{i}'])
                    dict_[row[f'ask_price_{i}']] = -row[f'ask_volume_{i}']
            order_depth_.sell_orders = dict_
            mid_price += [self.calculate_wavg_midprice(order_depth_)]

        for k, v in my_sell_hist.items():
            if k < start_timestamp:
                continue
            if k > end_timestamp:
                break
            for p, q in v.items():
                if display_my_order:
                    ax.scatter(k, p, c='cornflowerblue', marker='X', alpha=0.5, s=q*50)

        for k, v in my_buy_hist.items():
            if k < start_timestamp:
                continue
            if k > end_timestamp:
                break
            for p, q in v.items():
                if display_my_order:
                    ax.scatter(k, p, c='tomato', marker='X', alpha=0.5, s=q*50)

        for k, v in other_hist.items():
            if k < start_timestamp:
                continue
            if k > end_timestamp:
                break
            for p, q in v.items():
                if display_other_order:
                    ax.scatter(k, p, c='grey', marker='X', alpha=0.5, s=q*50)

        if display_mid_price:
            ax.plot(self.timestamp[start_timestamp:end_timestamp + 1] / 100, mid_price, '-o', c='lime')

        if display_pos:
            ax2 = ax.twinx()
            _ = ax2.plot(self.timestamp[start_timestamp:end_timestamp + 1] / 100, pos[start_timestamp+1:end_timestamp + 2])

    
    def plot_interactive(self, product, start_timestamp = 0, end_timestamp = 100, display_book=True, display_my_order=True, display_other_order=True):
        my_buy_hist, my_sell_hist, other_hist = defaultdict(lambda : defaultdict(float)), defaultdict(lambda : defaultdict(float)), defaultdict(lambda : defaultdict(float))
        cnt_buy = cnt_sell = 0
        for l in self.transaction_log:
            if l.symbol == product:
                if l.seller == "SUBMISSION":
                    my_sell_hist[l.timestamp // 100][l.price] += l.quantity
                    cnt_sell += 1
                elif l.buyer == "SUBMISSION":
                    my_buy_hist[l.timestamp // 100][l.price] += l.quantity
                    cnt_buy += 1
                else:
                    other_hist[l.timestamp // 100][l.price] += l.quantity
                     
        pos = [0]
        for ts in self.timestamp:
            cur, idx =0, ts / 100
            if idx in my_buy_hist:
                for _, v in my_buy_hist[idx].items():
                    cur += v
            if idx in my_sell_hist:
                for _, v in my_sell_hist[idx].items():
                    cur -= v
            pos += [pos[-1] + cur]

        df = self.df[self.df['product'] == product]
        df.reset_index(inplace=True)

        order_depth_ = self.OrderDepth()
        memo = pd.DataFrame(columns=['idx', 'Price', 'Quantity', 'Type 1', 'Type 2'])

        for _, row in df.iterrows():

            idx = row['timestamp'] // 100
            if idx < start_timestamp:
                continue
            if idx > end_timestamp:
                break

            idx = row['timestamp'] // 100
            dict_ = dict()
            for i in (1, 2, 3):
                if row[f'bid_volume_{i}'] != 0 and not np.isnan(row[f'bid_volume_{i}']):
                    if display_book:
                        memo.loc[len(memo)] = [idx, row[f'bid_price_{i}'], row[f'bid_volume_{i}'], 'Bid Book', '1']
                    dict_[row[f'bid_price_{i}']] = row[f'bid_volume_{i}']
            order_depth_.buy_orders = dict_
            dict_ = dict()
            for i in (1, 2, 3):
                if row[f'ask_volume_{i}'] != 0 and not np.isnan(row[f'ask_volume_{i}']):
                    if display_book:
                        memo.loc[len(memo)] = [idx, row[f'ask_price_{i}'], row[f'ask_volume_{i}'], 'Ask Book', '1']
                    dict_[row[f'ask_price_{i}']] = -row[f'ask_volume_{i}']
            order_depth_.sell_orders = dict_


        for k, v in my_sell_hist.items():
            if k < start_timestamp:
                continue
            if k > end_timestamp:
                break
            for p, q in v.items():
                if display_my_order:
                    memo.loc[len(memo)] = [k, p, q, 'My Ask', '2']

        for k, v in my_buy_hist.items():
            if k < start_timestamp:
                continue
            if k > end_timestamp:
                break
            for p, q in v.items():
                if display_my_order:
                    memo.loc[len(memo)] = [k, p, q, 'My Bid', '2']

        for k, v in other_hist.items():
            if k < start_timestamp:
                continue
            if k > end_timestamp:
                break
            for p, q in v.items():
                if display_other_order:
                    memo.loc[len(memo)] = [k, p, q, 'Others', '2']

        fig = px.scatter(memo, x="idx", y="Price", color="Type 1", size='Quantity', symbol='Type 2', width=1800, height=800)
        fig.show()
        return memo


    def plot_pnl(self, product, start_timestamp = 0, end_timestamp = 10000):

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Position
        my_buy_hist, my_sell_hist = defaultdict(lambda : defaultdict(float)), defaultdict(lambda : defaultdict(float))
        cnt_buy = cnt_sell = 0
        for l in self.transaction_log:
            if l.symbol == product:
                if l.seller == "SUBMISSION":
                    my_sell_hist[l.timestamp // 100][l.price] += l.quantity
                    cnt_sell += 1
                elif l.buyer == "SUBMISSION":
                    my_buy_hist[l.timestamp // 100][l.price] += l.quantity
                    cnt_buy += 1
                     
        pos = [0]
        for ts in self.timestamp:
            cur, idx =0, ts / 100
            if idx in my_buy_hist:
                for _, v in my_buy_hist[idx].items():
                    cur += v
            if idx in my_sell_hist:
                for _, v in my_sell_hist[idx].items():
                    cur -= v
            pos += [pos[-1] + cur]

        chart_idx = np.arange(start_timestamp, min(end_timestamp, self.timestamp[-1] / 100 + 1), 1)
        fig.add_trace(go.Scatter(x=chart_idx, y=pos[start_timestamp+1:end_timestamp+1], name="Position"), secondary_y=False)

        # PnL
        df = self.df[self.df['product'] == product]
        df.reset_index(inplace=True)
        pnl = [0]
        for _, row in df.iterrows():
            idx = row['timestamp'] // 100
            if idx < start_timestamp:
                    continue
            if idx > end_timestamp:
                    break

            pnl += [row["profit_and_loss"]]

        fig.add_trace(go.Scatter(x=chart_idx, y=pnl[start_timestamp+1:end_timestamp+1], name="PnL"), secondary_y=True)
        fig.show()




class VisualizerRaw:

    class tradelog:
        def __init__(self, log):
            self._dict = log
        def __getattr__(self, w):
            return self._dict.get(w, "")

    class OrderDepth:
        def __init__(self):
                self.buy_orders = None
                self.sell_orders = None

    def __init__(self, prices_csv_file, trades_csv_file):

        self.df_prices = pd.read_csv(prices_csv_file, sep=";")
        self.timestamp = np.arange(0, self.df_prices['timestamp'].max() + 100, 100)
        self.df_trades = pd.read_csv(trades_csv_file, sep=";")


    @staticmethod
    def calculate_wavg_midprice(order_depth):
        sorted_buy_orders = sorted(order_depth.buy_orders.items(), reverse=True)
        sorted_sell_orders = sorted(order_depth.sell_orders.items(), reverse=False)
        total_volume = 0
        wavg_price = 0
        for p, vol in sorted_buy_orders:
                wavg_price += p * vol
                total_volume += vol
        for p, vol in sorted_sell_orders:
                vol = abs(vol)
                wavg_price += p * vol
                total_volume += vol
        wavg_price /= total_volume
        return wavg_price

    
    def plot_interactive(self, product, start_timestamp = 0, end_timestamp = 100, display_book=True, display_order=True):
        
        other_hist = defaultdict(lambda : defaultdict(float))
        df = self.df_trades[self.df_trades['symbol'] == product]
        for _, row in df.iterrows():
            other_hist[row['timestamp'] / 100][row['price']] += row['quantity']
                     
        df = self.df_prices[self.df_prices['product'] == product]
        df.reset_index(inplace=True)

        order_depth_ = self.OrderDepth()
        memo = pd.DataFrame(columns=['idx', 'Price', 'Quantity', 'Type 1', 'Type 2'])

        for _, row in df.iterrows():

            idx = row['timestamp'] // 100
            if idx < start_timestamp:
                continue
            if idx > end_timestamp:
                break

            idx = row['timestamp'] // 100
            dict_ = dict()
            for i in (1, 2, 3):
                if row[f'bid_volume_{i}'] != 0 and not np.isnan(row[f'bid_volume_{i}']):
                    if display_book:
                        memo.loc[len(memo)] = [idx, row[f'bid_price_{i}'], row[f'bid_volume_{i}'], 'Bid Book', '1']
                    dict_[row[f'bid_price_{i}']] = row[f'bid_volume_{i}']
            order_depth_.buy_orders = dict_
            dict_ = dict()
            for i in (1, 2, 3):
                if row[f'ask_volume_{i}'] != 0 and not np.isnan(row[f'ask_volume_{i}']):
                    if display_book:
                        memo.loc[len(memo)] = [idx, row[f'ask_price_{i}'], row[f'ask_volume_{i}'], 'Ask Book', '1']
                    dict_[row[f'ask_price_{i}']] = -row[f'ask_volume_{i}']
            order_depth_.sell_orders = dict_

        for k, v in other_hist.items():
            if k < start_timestamp:
                continue
            if k > end_timestamp:
                break
            for p, q in v.items():
                if display_order:
                    memo.loc[len(memo)] = [k, p, q, 'Others', '2']

        fig = px.scatter(memo, x="idx", y="Price", color="Type 1", size='Quantity', symbol='Type 2', width=1800, height=800)
        fig.show()
        return memo

