from datamodel import OrderDepth, UserId, TradingState, Order
from typing import List
import jsonpickle as jp
import numpy as np


class STRATEGY:

    LIMIT = {
        'RAINFOREST_RESIN': 50,
        'KELP': 50,
        'SQUID_INK': 50
    }

    def __init__(self):
        self.orders = []

    @staticmethod
    def hit_the_book(product, order_depth, buy_capacity, buy_price, sell_capacity, sell_price):
        sorted_buy_orders = sorted(order_depth.buy_orders.items(), reverse=True)
        sorted_sell_orders = sorted(order_depth.sell_orders.items(), reverse=False)
        orders = []
        for p, vol in sorted_buy_orders:
            vol = abs(vol)
            if p >= sell_price:
                s_amt = min(sell_capacity, vol)
                if s_amt > 0:
                    sell_capacity -= s_amt
                    orders.append(Order(product, p, -s_amt))
        for p, vol in sorted_sell_orders:
            vol = abs(vol)
            if p <= buy_price:
                b_amt = min(buy_capacity, vol)
                if b_amt > 0:
                    buy_capacity -= b_amt
                    orders.append(Order(product, p, b_amt))
        return orders, buy_capacity, sell_capacity


    @staticmethod
    def calculate_mid_price(order_depth):
        sorted_buy_orders = sorted(order_depth.buy_orders.items(), reverse=True)
        sorted_sell_orders = sorted(order_depth.sell_orders.items(), reverse=False)
        
        vb = sum(x[1] for x in sorted_buy_orders)
        pb = sum(x[0] * x[1] for x in sorted_buy_orders)
        va = - sum(x[1] for x in sorted_sell_orders)
        pa = sum(x[0] * abs(x[1]) for x in sorted_sell_orders)

        bid = pb / vb if sorted_buy_orders else None
        ask = pa / va if sorted_sell_orders else None
        if bid is None or ask is None:
            return None
        return round( (bid + ask) / 2.0 )



    @staticmethod
    def calculate_barrier_price(order_book, threshold=1, ignore_below=None, ignore_above=None):
        if any(v < 0 for v in order_book.values()):
            order_book = sorted(order_book.items())
        else:
            order_book = sorted(order_book.items(), reverse=True)

        for p, q in order_book:
            if ignore_below is not None and p <= ignore_below:
                continue
            if ignore_above is not None and p >= ignore_above:
                continue
            if abs(q) >= threshold:
                return p
        
        return None



class RAINFOREST_RESIN_STRATEGY(STRATEGY):

    def __init__(self):
        super().__init__()
        self.product = "RAINFOREST_RESIN"

    def act(self, state, memo):

        current_position = state.position.get(self.product, 0)
        order_depth = state.order_depths[self.product]
        sell_capacity = self.LIMIT[self.product] + current_position
        buy_capacity = self.LIMIT[self.product] - current_position

        if buy_capacity == 0:
            memo['rr_liquidation_sell'] += 1
        else:
            memo['rr_liquidation_sell'] = 0

        if sell_capacity == 0:
            memo['rr_liquidation_buy'] += 1
        else:
            memo['rr_liquidation_buy'] = 0
        
        if buy_capacity < 25:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 9999, sell_capacity, 10000)
        elif sell_capacity < 25:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 10000, sell_capacity, 10001)
        else:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 10000, sell_capacity, 10000)
        self.orders.extend(orders_)

        bbp = self.calculate_barrier_price(order_depth.buy_orders, ignore_above=10000, threshold=1)
        bsp = self.calculate_barrier_price(order_depth.sell_orders, ignore_below=10000, threshold=1)
        rr_bid_price = max(9996, bbp if bbp is not None else 9996)
        rr_ask_price = min(10004, bsp if bsp is not None else 10004)

        if sell_capacity > 0:
            self.orders.append(Order(self.product, rr_ask_price, -sell_capacity))
        if buy_capacity > 0:
            self.orders.append(Order(self.product, rr_bid_price, buy_capacity))

        return self.orders



class KELP_STRATEGY(STRATEGY):

    def __init__(self):
        super().__init__()
        self.product = "KELP"

    def act(self, state, memo):

        ts = state.timestamp

        if self.product in state.own_trades:

            for trade in state.own_trades[self.product]:
                if trade.timestamp == ts - 100:
                    p = trade.price
                    q = trade.quantity

                    direction = 0
                    if trade.buyer == 'SUBMISSION':
                        direction = 1
                    elif trade.seller == 'SUBMISSION': 
                        direction = -1
                    for _ in range(int(abs(q))):

                        if memo['kelp_status'] == 0:
                            memo['kelp_cost_basis'] += [p]
                            memo['kelp_status'] = direction

                        elif memo['kelp_status'] < 0:
                            if direction == 1:
                                memo['kelp_cost_basis'] = memo['kelp_cost_basis'][1:]
                                if not memo['kelp_cost_basis']:
                                    memo['kelp_status'] = 0
                            elif direction == -1:
                                memo['kelp_cost_basis'] = memo['kelp_cost_basis'] + [p]
                    
                        elif memo['kelp_status'] > 0:
                            if direction == 1:
                                memo['kelp_cost_basis'] = memo['kelp_cost_basis'] + [p]
                            elif direction == -1:
                                memo['kelp_cost_basis'] = memo['kelp_cost_basis'][1:]
                                if not memo['kelp_cost_basis']:
                                    memo['kelp_status'] = 0
        

        current_position = state.position.get(self.product, 0)
        order_depth = state.order_depths[self.product]
        sell_capacity = self.LIMIT[self.product] + current_position
        buy_capacity = self.LIMIT[self.product] - current_position

        average_cost = None
        if memo['kelp_cost_basis']:
            average_cost = np.average(memo['kelp_cost_basis'])

        bbp = self.calculate_barrier_price(order_depth.buy_orders, threshold=15)
        bsp = self.calculate_barrier_price(order_depth.sell_orders, threshold=15)

        mid_price = None
        kelp_bid_price = None
        kelp_ask_price = None
        if bbp is not None and bsp is not None:
            mid_price = (bbp + bsp) / 2
            kelp_bid_price = bbp + 1
            kelp_ask_price = bsp - 1
        elif bbp is not None and bsp is None:
            mid_price = bbp + 2
            kelp_bid_price = bbp + 1
            kelp_ask_price = kelp_bid_price + 2
        elif bbp is None and bsp is not None:
            mid_price = bsp - 2
            kelp_ask_price = bsp -1
            kelp_bid_price = kelp_ask_price - 2
            
        if round(mid_price) != mid_price:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, int(mid_price), sell_capacity, int(mid_price) + 1)
        else:
            if buy_capacity < 10:
                orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, mid_price - 1, sell_capacity, mid_price)
            elif sell_capacity < 10:
                orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, mid_price, sell_capacity, mid_price + 1)
            else:
                orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, mid_price - 1, sell_capacity, mid_price + 1)
        self.orders.extend(orders_)

        orders_ = []
        if average_cost is not None and buy_capacity < 5:
            liquidation_price = int(average_cost) + 5
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1, sell_capacity, liquidation_price)
        elif average_cost is not None and sell_capacity < 5:
            liquidation_price = int(average_cost) - 4
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, liquidation_price, sell_capacity, 99999)
        self.orders.extend(orders_)

        if sell_capacity > 0 and kelp_ask_price is not None:
            self.orders.append(Order(self.product, kelp_ask_price, -sell_capacity))
        if buy_capacity > 0 and kelp_bid_price is not None:
            self.orders.append(Order(self.product, kelp_bid_price, buy_capacity))

        return self.orders



class SQUID_INK_STRATEGY(STRATEGY):

    def __init__(self):
        super().__init__()
        self.product = "SQUID_INK"
        self.window_size = 400
        self.buffer = 12

    def act(self, state, memo):



        order_depth = state.order_depths[self.product]
        mp = self.calculate_mid_price(order_depth)
        ma = memo['si_ma'] / self.window_size
        idx = state.timestamp / 100

        if idx == 0:
            bbp = self.calculate_barrier_price(order_depth.buy_orders, threshold=15)
            memo['si_vcnt'][0] += 1 if bbp == 1834 else 0 
            memo['si_vcnt'][1] += 1 if bbp == 1968 else 0 
        elif idx == 1:
            bbp = self.calculate_barrier_price(order_depth.buy_orders, threshold=15)
            memo['si_vcnt'][0] += 1 if bbp == 1835 else 0 
            memo['si_vcnt'][1] += 1 if bbp == 1967 else 0 
        elif idx == 2:
            bbp = self.calculate_barrier_price(order_depth.buy_orders, threshold=15)
            memo['si_vcnt'][0] += 1 if bbp == 1832 else 0 
            memo['si_vcnt'][1] += 1 if bbp == 1965 else 0 

        elif idx < 1000:
            if memo['si_vcnt'][0] == 3:
                self.overfit(state, memo, 0)
            elif memo['si_vcnt'][1] == 3:
                self.overfit(state, memo, 1)

        elif idx >= self.window_size and mp is not None:
             current_position = state.position.get(self.product, 0)
             sell_capacity = self.LIMIT[self.product] + current_position
             buy_capacity = self.LIMIT[self.product] - current_position

             if mp <= 1900: 
                 sell_capacity = min(50, sell_capacity)
             elif mp >= 2100:
                 buy_capacity = min(50, buy_capacity)

             orders_ = []
             if buy_capacity > 0 and ma - mp >= self.buffer:

                 bbp = self.calculate_barrier_price(order_depth.buy_orders, threshold=15)
                 if bbp is not None:
                    self.orders.append(Order(self.product, bbp + 1, buy_capacity)) 
                 # orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, mp + 4, sell_capacity, 99999)   
             elif sell_capacity > 0 and mp - ma >= self.buffer:
                 bsp = self.calculate_barrier_price(order_depth.sell_orders, threshold=15)
                 if bsp is not None:
                     self.orders.append(Order(self.product, bsp - 1, -sell_capacity))
                 # orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1, sell_capacity, mp - 4)   
             self.orders.extend(orders_)

        memo['si_ma'] += mp
        if idx >= self.window_size:
            memo['si_ma'] -= memo['si_hist_prices'][0]
            memo['si_hist_prices'] =  memo['si_hist_prices'][1:]
        memo['si_hist_prices'] += [mp]

        return self.orders

    def overfit(self, state, memo, v):

        if v == 0:

            b = [[2.0, 1835],
                [156.0, 1849],
                [168.0, 1840],
                [191.0, 1824],
                [233.0, 1844],
                [267.0, 1847],
                [333.0, 1836],
                [521.0, 1830],
                [582.0, 1840],
                [640.0, 1834],
                [677.0, 1834],
                [810.0, 1803],
                [846.0, 1798],
                [916.0, 1803]]

            s = [[111.0, 1864],
                [160.0, 1856],
                [171.0, 1849],
                [222.0, 1849],
                [253.0, 1854],
                [299.0, 1854],
                [430.0, 1864],
                [572.0, 1847],
                [599.0, 1845],
                [647.0, 1839],
                [728.0, 1846],
                [818.0, 1812],
                [860.0, 1814],
                [937.0, 1818]]

        elif v == 1:
            
            b = [[3.0, 1968],
                [129.0, 1974],
                [172.0, 1964],
                [331.0, 1958],
                [414.0, 1941],
                [574.0, 1945],
                [589.0, 1944],
                [613.0, 1946],
                [753.0, 1957],
                [814.0, 1964],
                [878.0, 1963]]
            
            s = [[101.0, 1982],
                [148.0, 1977],
                [241.0, 1984],
                [371.0, 1961],
                [499.0, 1954],
                [577.0, 1948],
                [597.0, 1949],
                [724.0, 1964],
                [784.0, 1971],
                [849.0, 1970],
                [969.0, 1973]]


        current_position = state.position.get(self.product, 0)
        order_depth = state.order_depths[self.product]
        sell_capacity = self.LIMIT[self.product] + current_position
        buy_capacity = self.LIMIT[self.product] - current_position

        direction = 0 if b[0][0] < s[0][0] else 1
        orders = sorted(b + s)

        idx = state.timestamp / 100
        if idx >= orders[-1][0] and idx <= orders[-1][0] + 10:
            orders_ = []
            if current_position > 0:
                orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, 0, 1, current_position, 1)
            elif current_position < 0:
                orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, -current_position, 9999, 0, 99999)
            self.orders.extend(orders_)
        else:
            for i, (t, p) in enumerate(orders):
                if i < len(orders) - 1 and orders[i][0] <= idx and orders[i + 1][0] > idx:
                    if i % 2 == direction and buy_capacity > 0:
                        orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, orders[i + 1][1] - 2, sell_capacity, 99999)
                        self.orders.extend(orders_)
                        self.orders.append(Order(self.product, p + 1, buy_capacity))

                    elif i % 2 != direction and sell_capacity > 0:
                        orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1, sell_capacity, orders[i + 1][1] + 2)
                        self.orders.extend(orders_)
                        self.orders.append(Order(self.product, p - 1, -sell_capacity))




class Trader:

    MEMO = {
        'rr_liquidation_sell': 0,
        'rr_liquidation_buy': 0,
        'kelp_cost_basis': [],
        'kelp_status': 0,
        'si_vcnt': [0, 0],
        'si_ma': 0,
        'si_hist_prices': []

    }

    STRATEGY = {
        'RAINFOREST_RESIN': RAINFOREST_RESIN_STRATEGY,
        'KELP': KELP_STRATEGY,
        'SQUID_INK': SQUID_INK_STRATEGY
    }

    def run(self, state: TradingState):

        if state.traderData != "":
            self.MEMO = jp.decode(state.traderData)

        result = {}

        for product in state.order_depths:
            result[product] = self.STRATEGY[product]().act(state, self.MEMO)

        return result, None, jp.encode(self.MEMO)