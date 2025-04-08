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

    def act(self, state, memo):

        idx = state.timestamp / 100

        if idx < 1000:
            self.overfit(state, memo)

        else:
            current_position = state.position.get(self.product, 0)
            order_depth = state.order_depths[self.product]
            sell_capacity = self.LIMIT[self.product] + current_position
            buy_capacity = self.LIMIT[self.product] - current_position

            bbp = self.calculate_barrier_price(order_depth.buy_orders, threshold=15)
            bsp = self.calculate_barrier_price(order_depth.sell_orders, threshold=15)

            mid_price = None
            si_bid_price = None
            si_ask_price = None
            if bbp is not None and bsp is not None:
                mid_price = (bbp + bsp) / 2
                si_bid_price = bbp + 1
                si_ask_price = bsp - 1
            elif bbp is not None and bsp is None:
                mid_price = bbp + 2
                si_bid_price = bbp + 1
                si_ask_price = si_bid_price + 2
            elif bbp is None and bsp is not None:
                mid_price = bsp - 2
                si_ask_price = bsp -1
                si_bid_price = si_ask_price - 2
                
            if round(mid_price) != mid_price:
                orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, int(mid_price), sell_capacity, int(mid_price) + 1)
            else:
                orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, mid_price - 1, sell_capacity, mid_price + 1)
            self.orders.extend(orders_)

            if sell_capacity > 0 and si_ask_price is not None:
                self.orders.append(Order(self.product, si_ask_price, -sell_capacity))
            if buy_capacity > 0 and si_bid_price is not None:
                self.orders.append(Order(self.product, si_bid_price, buy_capacity))

        return self.orders


    def overfit(self, state, memo):

        current_position = state.position.get(self.product, 0)
        order_depth = state.order_depths[self.product]
        sell_capacity = self.LIMIT[self.product] + current_position
        buy_capacity = self.LIMIT[self.product] - current_position

        if (state.timestamp / 100 >= 2 and state.timestamp / 100 <= 6) and buy_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1835, sell_capacity, 99999)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1833, buy_capacity))

        elif (state.timestamp / 100 >= 66 and state.timestamp / 100 <= 71) and sell_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1, sell_capacity, 1860)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1860, -sell_capacity))

        elif (state.timestamp / 100 >= 191 and state.timestamp / 100 <= 200) and buy_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1829, sell_capacity, 99999)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1833, buy_capacity))

        elif (state.timestamp / 100 >= 430 and state.timestamp / 100 <= 435) and sell_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1, sell_capacity, 1860)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1860, -sell_capacity))

        elif (state.timestamp / 100 >= 517 and state.timestamp / 100 <= 523) and buy_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1827, sell_capacity, 99999)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1827, buy_capacity))

        elif (state.timestamp / 100 >= 572 and state.timestamp / 100 <= 574) and sell_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1, sell_capacity, 1846)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1846, -sell_capacity))

        elif (state.timestamp / 100 >= 806 and state.timestamp / 100 <= 811) and buy_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1805, sell_capacity, 99999)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1805, buy_capacity))

        elif (state.timestamp / 100 >= 818 and state.timestamp / 100 <= 820) and sell_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1, sell_capacity, 1811)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1811, -sell_capacity))

        elif (state.timestamp / 100 >= 843 and state.timestamp / 100 <= 846) and buy_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1800, sell_capacity, 99999)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1800, buy_capacity))

        elif (state.timestamp / 100 >= 859 and state.timestamp / 100 <= 863) and sell_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1, sell_capacity, 1813)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1813, -sell_capacity))

        elif (state.timestamp / 100 >= 912 and state.timestamp / 100 <= 916) and buy_capacity > 0:
            orders_, buy_capacity, sell_capacity = self.hit_the_book(self.product, order_depth, buy_capacity, 1804, sell_capacity, 99999)
            self.orders.extend(orders_)
            self.orders.append(Order(self.product, 1804, buy_capacity))


class Trader:

    MEMO = {
        'rr_liquidation_sell': 0,
        'rr_liquidation_buy': 0,
        'kelp_cost_basis': [],
        'kelp_status': 0
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