from dataclasses import dataclass
from collections import defaultdict
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datamodel
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


POSITION_LIMIT = {
    'RAINFOREST_RESIN': 50,
    'KELP': 50
}


@dataclass
class result:
    """Class for simulation result"""
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    pnl: defaultdict
    transactions: pd.DataFrame
    mid_price: defaultdict
    position: defaultdict

    def summary(self, verbose=False):

        product_list = list(self.pnl.keys())
        product_list.remove('total')

        for k, v in self.pnl.items():
            logging.info(f" -> {k}: {v[-1]:.1f}")
        
        logging.info('\n')

        total_transaction_amt = len(self.transactions)        
        logging.info(f"# of transaction is {total_transaction_amt:d}")
        for k in product_list:
            buy = self.transactions[(self.transactions['Product'] == k) & (self.transactions['B/S'] == 'B')]
            sell = self.transactions[(self.transactions['Product'] == k) & (self.transactions['B/S'] == 'S')]
            total_v = len(buy) + len(sell)
            logging.info(f" -> {k}: B({len(buy):>3d}) + S({len(sell):>3d})")

        if not verbose:
            fig, ax = plt.subplots(1, 1)
            legends = []
            for k, v in self.pnl.items():
                ax.set_title("PnL")
                ax.plot(v)
                legends.append(k)
            ax.legend(legends)

        else:
            nrow = len(self.pnl) - 1
            fig, axs = plt.subplots(nrow, 2, figsize=(20, 12))
            for i, (k, v) in enumerate(self.pnl.items()):
                if k != 'total':
                    axs[i][0].set_title(k)
                    axs[i][0].plot(v)   
                    axs[i][0].legend(["Pnl"])
              
                    axs[i][1].set_title(k)
                    axs[i][1].plot(self.position[k])
                    ax2 = axs[i][1].twinx()
                    ax2.plot(self.mid_price[k], c='orange', alpha=0.5)
                    axs[i][1].set_ylim([-POSITION_LIMIT[k], POSITION_LIMIT[k]])

    
    @staticmethod
    def compare(r1, r2, product, display_buy=True, display_sell=True):

        r1 = r1.transactions[r1.transactions['Product'] == product]
        r2 = r2.transactions[r2.transactions['Product'] == product]

        r1['idx'] = r1['TimeStamp'] / 100
        r2['idx'] = r2['TimeStamp'] / 100
        r1['Type 1'] = "r1"
        r2['Type 1'] = "r2"
        r1['Type 2'] = r1['B/S']
        r2['Type 2'] = r2['B/S']

        if not display_buy:
            r1 = r1[r1['Type 2'] == 'S']
            r2 = r2[r2['Type 2'] == 'S']
        elif not display_sell:
            r1 = r1[r1['Type 2'] == 'B']
            r2 = r2[r2['Type 2'] == 'B']

        r = pd.concat([r1, r2])

        fig = px.scatter(r, x="idx", y="Price", color="Type 1", size='Quantity', symbol='Type 2', width=1800, height=800)
        fig.show()


    @staticmethod
    def compare_diff(r1, r2, product, display_buy=True, display_sell=True):

        r1 = r1.transactions[r1.transactions['Product'] == product]
        r2 = r2.transactions[r2.transactions['Product'] == product]

        tmp = r1.merge(r2, how='outer', indicator=True)
        diff = tmp[tmp['_merge'] != 'both']
        diff['Type 1'] = diff['_merge'].apply(lambda x: 'r1' if x =='left_only' else 'r2')
        diff['Type 2'] = diff['B/S']
        diff['idx'] = diff['TimeStamp'] / 100
        
        if not display_buy:
            diff = diff[diff['Type 2'] == 'S']
        elif not display_sell:
            diff = diff[diff['Type 2'] == 'B']

        fig = px.scatter(diff, x="idx", y="Price", color="Type 1", size='Quantity', symbol='Type 2', width=1800, height=800)
        fig.show()



class Simulator:

    logging.basicConfig(format='%(message)s', level=logging.INFO)

    def __init__(self, csv_file=None, log_file=None, sep=';'):

        if csv_file is not None:
            self.df = pd.read_csv(csv_file, sep=sep)
        elif log_file is not None:
            self.df = self._read_log(log_file)

        self.product = self.df['product'].unique().tolist()
        self.last_price_ = defaultdict(list)

        self.traderdata = ""
        self.timestamp = np.arange(0, self.df['timestamp'].max() + 100, 100)
        self.order_depths_ = dict()
        self.own_trades_ = dict()
        self.listings_ = dict()
        self.market_trades_ = dict()
        self.position_ = defaultdict(lambda: [0])
        self.observations_ = dict() 
        self.pnl_ = defaultdict(lambda: [0])
        self.cost_basis_ = defaultdict(float)
        self.records = pd.DataFrame(columns=['TimeStamp', 'B/S', 'Quantity', "Product", 'Price'])


    @staticmethod
    def _read_log(log_file, tmp_file=r"/Users/ysc/Desktop/imcProsperity/cache/tmp_.csv"):
        with open(log_file) as f:
            tmp = f.read()
            start = tmp.find("Activities log:") + 16
            end = tmp.find("\n\n\n\n", start)
            tmp = tmp[start:end]
        with open(tmp_file, mode='w') as f:
            f.write(tmp)
        return pd.read_csv(tmp_file, sep=";")


    def _clear(self):
        self.order_depths_.clear()
        self.listings_.clear()
        self.market_trades_.clear()
        self.observations_.clear() 


    def _reset(self):
        self._clear()
        self.traderdata = ""
        self.own_trades_.clear()
        self.position_ = defaultdict(lambda: [0])
        self.pnl_ = defaultdict(lambda: [0])
        self.cost_basis_ = defaultdict(float)
        self.records = pd.DataFrame(columns=['TimeStamp', 'B/S', 'Quantity', "Product", 'Price'])


    def _breach_or_not(self):
        breach_item = []
        for k, v in POSITION_LIMIT.items():
            if abs(self.position_[k][-1]) > v:
                breach_item += [k]
        return breach_item


    def _settle(self, orders, timestamp):

        self.own_trades_.clear()

        for product in self.product:
            
            new_position = self.position_[product][-1]

            if product in orders:
                
                order_depth = self.order_depths_[product]
                order = orders[product]

                sorted_buy_orders = list(map(list, sorted(order_depth.buy_orders.items(), reverse=True)))
                sorted_sell_orders = list(map(list, sorted(order_depth.sell_orders.items(), reverse=False)))

                for o in order:
                    price, quantity = o.price, o.quantity
                    
                    if quantity > 0:
                        for v in sorted_sell_orders:
                            if price >= v[0] and abs(v[1]) > 0 and quantity > 0:
                                done = min(quantity, abs(v[1]))
                                quantity -= done
                                new_position += done 
                                if product not in self.own_trades_:
                                    self.own_trades_[product] = [datamodel.Trade(product, v[0], done, self.traderdata, "", timestamp)]
                                else:
                                    self.own_trades_[product].append(datamodel.Trade(product, v[0], done, self.traderdata, "", timestamp))
                                self.cost_basis_[product] -= done * v[0]
                                self.records.loc[len(self.records)] = timestamp, 'B', done, product, v[0]
                                v[1] += done

                    elif quantity < 0:
                        for v in sorted_buy_orders:
                            if price <= v[0] and v[1] > 0 and quantity < 0:
                                done = min(abs(quantity), v[1])
                                quantity += done
                                new_position -= done 
                                if product not in self.own_trades_:
                                    self.own_trades_[product] = [datamodel.Trade(product, v[0], -done, "", self.traderdata, timestamp)]
                                else:
                                    self.own_trades_[product].append(datamodel.Trade(product, v[0], -done, "", self.traderdata, timestamp))
                                self.cost_basis_[product] += done * v[0]
                                self.records.loc[len(self.records)] = timestamp, 'S', done, product, v[0]
                                v[1] -= done
            
            self.position_[product] += [new_position]
            self.pnl_[product] += [ self.cost_basis_[product] + self.last_price_[product][-1] * self.position_[product][-1] ]

        self.pnl_['total'] += [ sum(self.pnl_[product][-1] for product in self.product) ]


    def simulate(self, Trader):

        cur_state = ""
        self._reset()

        for timestamp_ in self.timestamp:
            
            self._clear()

            tmp = self.df[self.df['timestamp'] == timestamp_]
            for _, row in tmp.iterrows():
                
                product_ = row['product']
                self.last_price_[product_] += [row['mid_price']]

                self.listings_[product_] = datamodel.Listing(product_, product_, 'SEASHELLS')
                self.order_depths_[product_] = datamodel.OrderDepth()

                dict_ = dict()
                for i in (1, 2, 3):
                    if row[f'bid_volume_{i}'] != 0 and not np.isnan(row[f'bid_volume_{i}']):
                        dict_[row[f'bid_price_{i}']] = row[f'bid_volume_{i}']
                self.order_depths_[product_].buy_orders = dict_
                
                dict_ = dict()
                for i in (1, 2, 3):
                    if row[f'ask_volume_{i}'] != 0 and not np.isnan(row[f'ask_volume_{i}']):
                        dict_[row[f'ask_price_{i}']] = -row[f'ask_volume_{i}']
                self.order_depths_[product_].sell_orders = dict_

            position__ = {k: v[-1] for k, v in self.position_.items()}

            tradingstate_ = datamodel.TradingState(cur_state, timestamp_, self.listings_, self.order_depths_, self.own_trades_, self.market_trades_, position__, self.observations_)
            orders, _, cur_state = Trader.run(tradingstate_)

            self._settle(orders, timestamp_)

            breach_test = self._breach_or_not()
            if breach_test:
                logging.error(f"-> Breach Occurs at timestamp {timestamp_} for {breach_test}")
                return None
        
        return result(self.pnl_, self.records, self.last_price_, self.position_)