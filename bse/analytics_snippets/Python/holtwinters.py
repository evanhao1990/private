"""
Implementation of Holt Winters Exponential Smoothing methods with scipy optimization.

References:
    https://www.otexts.org/fpp/7/6
    https://www.itl.nist.gov/div898/handbook/pmc/section4/pmc435.htm
    https://gist.github.com/andrequeiroz/5888967 # for optimization logic / layout

By Rory Vigus 2018-03-09

"""

import pandas as pd
import numpy as np
from scipy.optimize import fmin_l_bfgs_b


class HoltWinters(object):
    """ Holt winters forecasting methods. Forecasts h steps ahead from the end of the input series.

    params: optimizer results
    l: level series
    b: trend series
    s: seasonal series

    """

    def __init__(self):
        self.params = None
        self.l = None
        self.b = None
        self.s = None


    def _load_test_data(self):
        """Loads some test data in the form of a series with date index.

        """

        rng = pd.date_range('2013-01-01', end='2016-12-01', freq='MS')

        demand = [2936538.1689999998, 2313500.463, 2931951.1769999997,
                  3223802.9249999998, 2908916.23,
                  3143870.0930000003, 2745453.5019999999, 2442697.2990000001,
                  3536861.5600000001,
                  3609171.4330000002, 3931207.1680000001, 4207084.7510000002,
                  3119594.915, 2196489.648,
                  3117768.4639999997, 4353957.949, 3270190.4019999998,
                  2849031.4410000001, 2739589.673, 2542868.912,
                  2967221.8319999999, 3740479.5249999999, 4255619.4110000003,
                  3909943.9910000004, 3071769.608, 2445535.9509999999,
                  3184543.1060000001, 3321348.497, 3389815.9539999999,
                  3208558.682, 2623974.2919999999, 2404066.5469999998,
                  3542941.5380000002, 3897231.9389999998, 4720089.1680000005,
                  3612348.8369999998, 3057094.9680000003, 2530830.3030000003,
                  2958445.8689999999, 3291601.233, 3343578.1860000002,
                  3232688.2489999998, 2804867.358, 2375820.9440000001,
                  2985585.0039999997, 4355517.2080000006, 6030412.8710000003,
                  4533079.1009999998]

        yt = pd.Series(data=demand, index=rng)

        return yt


    def sse(self, params, *args):
        """Sum of squared error."""

        yt = args[0]
        mtype = args[1]
        season = args[2]
        sse = 0

        if mtype == 'multiplicative':

            alpha, beta, gamma = params

            l = [sum(yt[:season]) / float(season)]
            b = [(sum(yt[season:season*2]) - sum(yt[:season])) / season ** 2]
            s = [yt[i] / l[0] for i in range(season)]

            # Forecast series
            ft = [(l[0] + b[0]) * s[0]]
            sse += (yt[0] - ft[0]) ** 2

            for i in range(1, len(yt)):

                if i < season:  # we already have 12 seasonal values
                    l.append(alpha * (yt[i] / s[i-season]) + (1 - alpha) * (l[i - 1] + b[i - 1]))
                    b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * b[i - 1])
                    ft.append((l[i-1] + b[i-1]) * s[i - season])

                else:
                    l.append(alpha * (yt[i] / s[i - season]) + (1 - alpha) * (l[i - 1] + b[i - 1]))
                    b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * b[i - 1])
                    s.append((gamma * yt[i] / (l[i - 1] + b[i - 1])) + (1 - gamma) * s[i - season])
                    ft.append((l[i-1] + b[i-1]) * s[i - season])

                sse += (yt[i] - ft[i]) ** 2

            return sse

        if mtype == 'additive':

            alpha, beta, gamma = params

            l = [sum(yt[:season]) / float(season)]
            b = [(sum(yt[season:season*2]) - sum(yt[:season])) / season ** 2]
            s = [yt[i] - l[0] for i in range(season)]

            # Forecast series
            ft = [(l[0] + 1 * b[0]) + s[0]]
            sse += (yt[0] - ft[0]) ** 2

            for i in range(1, len(yt)):

                if i < season:  # we already have 12 seasonal values
                    l.append(alpha * (yt[i] - s[i-season]) + (1 - alpha) * (l[i - 1] + b[i - 1]))
                    b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * b[i - 1])
                    ft.append((l[i-1] + b[i-1]) + s[i - season])

                else:
                    l.append(alpha * (yt[i] - s[i - season]) + (1 - alpha) * (l[i - 1] + b[i - 1]))
                    b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * b[i - 1])
                    s.append((gamma * yt[i] - (l[i - 1] + b[i - 1])) + (1 - gamma) * s[i - season])
                    ft.append((l[i-1] + b[i-1]) + s[i - season])

                sse += (yt[i] - ft[i]) ** 2

            return sse

        if mtype == 'multiplicative_damped':

            alpha, beta, gamma, delta = params

            l = [sum(yt[:season]) / float(season)]
            b = [(sum(yt[season:season*2]) - sum(yt[:season])) / season ** 2]
            s = [yt[i] / l[0] for i in range(season)]

            # Forecast series
            ft = [(l[0] + delta * b[0]) * s[0]]
            sse += (yt[0] - ft[0]) ** 2

            for i in range(1, len(yt)):

                if i < season:  # we already have 12 seasonal values
                    l.append(alpha * (yt[i] / s[i-season]) + (1 - alpha) * (l[i - 1] + (delta * b[i - 1])))
                    b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * delta * b[i - 1])
                    ft.append((l[i-1] + (delta * b[i-1])) * s[i - season])

                else:
                    l.append(alpha * (yt[i] / s[i - season]) + (1 - alpha) * (l[i - 1] + (delta * b[i - 1])))
                    b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * delta * b[i - 1])
                    s.append((gamma * yt[i] / (l[i - 1] + (delta * b[i - 1]))) + (1 - gamma) * s[i - season])
                    ft.append((l[i-1] + (delta * b[i-1])) * s[i - season])

                sse += (yt[i] - ft[i]) ** 2

            return sse

        if mtype == 'additive_damped':

            alpha, beta, gamma, delta = params

            l = [sum(yt[:season]) / float(season)]
            b = [(sum(yt[season:season * 2]) - sum(yt[:season])) / season ** 2]
            s = [yt[i] - l[0] for i in range(season)]

            # Forecast series
            ft = [(l[0] + delta * b[0]) + s[0]]
            sse += (yt[0] - ft[0]) ** 2

            for i in range(1, len(yt)):

                if i < season:  # we already have 12 seasonal values
                    l.append(alpha * (yt[i] - s[i - season]) + (1 - alpha) * (
                    l[i - 1] + delta * b[i - 1]))
                    b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * delta * b[i - 1])
                    ft.append((l[i - 1] + delta * b[i - 1]) + s[i - season])

                else:
                    l.append(alpha * (yt[i] - s[i - season]) + (1 - alpha) * (
                    l[i - 1] + delta * b[i - 1]))
                    b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * delta * b[i - 1])
                    s.append((gamma * yt[i] - (l[i - 1] + delta * b[i - 1])) + (1 - gamma) * s[
                        i - season])
                    ft.append((l[i - 1] + delta * b[i - 1]) + s[i - season])  # Imagine you are in t, wanting to predict t+1, thus use -1 for l, b.

                sse += (yt[i] - ft[i]) ** 2

            return sse


    def multiplicative(self, yt, season, horizon, alpha=None, beta=None, gamma=None):
        """Holt Winters Multiplicative Model.

        Multiplicative Trend (M) | Additive Seasonal (A)

        """

        if (alpha == None or beta == None or gamma == None):

            initial_values = np.array([0.0, 1.0, 0.0])
            boundaries = [(0, 1), (0, 1), (0, 1)]
            mtype = 'multiplicative'

            # Optimize
            parameters = fmin_l_bfgs_b(self.sse, x0=initial_values, args=(yt, mtype, season), bounds=boundaries, approx_grad=True)
            alpha, beta, gamma = parameters[0]
            self.params = parameters

        # Calculate initial level
        l = [sum(yt[:season]) / float(season)]

        # Calculate initial trend
        b = [(sum(yt[season:season*2]) - sum(yt[:season])) / season ** 2]

        # Calculate initial season
        s = [yt[i] / l[0] for i in range(season)]

        # Forecast list
        ft = [(l[0] + b[0]) * s[0]]

        for i in range(1, len(yt) + horizon):

            if i < season: # we already have 12 seasonal values
                l.append(alpha * (yt[i] / s[i-season]) + (1 - alpha) * (l[i - 1] + b[i - 1]))
                b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * b[i - 1])
                ft.append((l[i-1] + b[i-1]) * s[i - season])

            elif i >= len(yt):
                h = i - len(yt) + 1 # Forecast steps
                ft.append((l[-1] + h * b[-1]) * s[-12:][h % season])

            else:
                l.append(alpha * (yt[i] / s[i - season]) + (1 - alpha)*(l[i - 1] + b[i - 1]))
                b.append(beta * (l[i] - l[i-1]) + (1 - beta)*b[i-1])
                s.append((gamma * yt[i] / (l[i-1] + b[i-1])) + (1 - gamma)*s[i-season])
                ft.append((l[i - 1] + b[i - 1]) * s[i - season]) # Imagine you are in t, wanting to predict t+1, thus use -1 for l, b.

        self.l = l
        self.b = b
        self.s = s

        return yt, ft


    def additive(self, yt, season, horizon, alpha=None, beta=None, gamma=None):
        """Holt Winters Additive Model.

        Additive Trend (M) | Additive Seasonal (A)

        """

        if (alpha == None or beta == None or gamma == None):

            initial_values = np.array([0.0, 1.0, 0.0])
            boundaries = [(0, 1), (0, 1), (0, 1)]
            mtype = 'additive'

            # Optimize
            parameters = fmin_l_bfgs_b(self.sse, x0=initial_values, args=(yt, mtype, season), bounds=boundaries, approx_grad=True)
            alpha, beta, gamma = parameters[0]
            self.params = parameters

        # Calculate initial level
        l = [sum(yt[:season]) / float(season)]

        # Calculate initial trend
        b = [(sum(yt[season:season*2]) - sum(yt[:season])) / season ** 2]

        # Calculate initial season
        s = [yt[i] - l[0] for i in range(season)]

        # Forecast list
        ft = [(l[0] + b[0]) + s[0]]

        for i in range(1, len(yt) + horizon):

            if i < season: # we already have 12 seasonal values
                l.append(alpha * (yt[i] - s[i-season]) + (1 - alpha) * (l[i - 1] + b[i - 1]))
                b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * b[i - 1])
                ft.append((l[i-1] + b[i-1]) + s[i - season])

            elif i >= len(yt):
                h = i - len(yt) + 1 # Forecast steps
                ft.append((l[-1] + h * b[-1]) + s[-12:][h % season])

            else:
                l.append(alpha * (yt[i] - s[i-season]) + (1 - alpha)*(l[i - 1] + b[i - 1]))
                b.append(beta * (l[i] - l[i-1]) + (1 - beta)*b[i-1])
                s.append((gamma * yt[i] - (l[i-1] + b[i-1])) + (1 - gamma)*s[i-season])
                ft.append((l[i - 1] + b[i - 1]) + s[i - season]) # Imagine you are in t, wanting to predict t+1, thus use -1 for l, b.

        self.l = l
        self.b = b
        self.s = s

        return yt, ft


    def multiplicative_damped(self, yt, season, horizon, alpha=None, beta=None, gamma=None, delta=None):
        """Holt Winters Multiplicative-Damped Model.

        Multiplicative Damped Trend (MD) | Additive Seasonal (A)

        """

        if (alpha == None or beta == None or gamma == None or delta == None):

            initial_values = np.array([0.0, 1.0, 0.0, 0.5])
            boundaries = [(0, 1), (0, 1), (0, 1), (0, 1)]
            mtype = 'multiplicative_damped'

            # Optimize
            parameters = fmin_l_bfgs_b(self.sse, x0=initial_values, args=(yt, mtype, season), bounds=boundaries, approx_grad=True)
            alpha, beta, gamma, delta = parameters[0]
            self.params = parameters

        # Calculate initial level
        l = [sum(yt[:season]) / float(season)]

        # Calculate initial trend
        b = [(sum(yt[season:season*2]) - sum(yt[:season])) / season ** 2]

        # Calculate initial season
        s = [yt[i] / l[0] for i in range(season)]

        # Decaying delta function
        delta_process = np.cumsum(np.array(delta) ** np.arange(1, horizon + 1))

        # Forecast list
        ft = [(l[0] + delta * b[0]) * s[0]]

        for i in range(1, len(yt) + horizon):

            if i < season: # we already have 12 seasonal values
                l.append(alpha * (yt[i] / s[i-season]) + (1 - alpha) * (l[i - 1] + (delta * b[i - 1])))
                b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * delta * b[i - 1])
                ft.append((l[i-1] + delta * b[i-1]) * s[i - season])

            elif i >= len(yt):
                h = i - len(yt) + 1 # Forecast steps
                ft.append((l[-1] + h * delta_process[h - 1] * b[-1]) * s[-12:][h % season])

            else:
                l.append(alpha * (yt[i] / s[i - season]) + (1 - alpha)*(l[i - 1] + (delta * b[i - 1])))
                b.append(beta * (l[i] - l[i-1]) + (1 - beta) * delta * b[i-1])
                s.append((gamma * yt[i] / (l[i-1] + (delta * b[i-1]))) + (1 - gamma)*s[i-season])
                ft.append((l[i - 1] + delta * b[i - 1]) * s[i - season]) # Imagine you are in t, wanting to predict t+1, thus use -1 for l, b.

        self.l = l
        self.b = b
        self.s = s

        return yt, ft


    def additive_damped(self, yt, season, horizon, alpha=None, beta=None, gamma=None, delta=None):
        """Holt Winters Additive Damped Model.

        Additive Damped Trend (AD) | Additive Seasonal (A)

        """

        if (alpha == None or beta == None or gamma == None or delta == None):

            initial_values = np.array([0.0, 1.0, 0.0, 0.5])
            boundaries = [(0, 1), (0, 1), (0, 1), (0, 1)]
            mtype = 'additive_damped'

            # Optimize
            parameters = fmin_l_bfgs_b(self.sse, x0=initial_values, args=(yt, mtype, season), bounds=boundaries, approx_grad=True)
            alpha, beta, gamma, delta = parameters[0]
            self.params = parameters

        # Calculate initial level
        l = [sum(yt[:season]) / float(season)]

        # Calculate initial trend
        b = [(sum(yt[season:season*2]) - sum(yt[:season])) / season ** 2]

        # Calculate initial season
        s = [yt[i] - l[0] for i in range(season)]

        # Forecast list
        ft = [(l[0] + delta * b[0]) + s[0]]

        # Decaying delta function
        delta_process = np.cumsum(np.array(delta) ** np.arange(1, horizon + 1))

        for i in range(1, len(yt) + horizon):

            if i < season: # we already have 12 seasonal values
                l.append(alpha * (yt[i] - s[i-season]) + (1 - alpha) * (l[i - 1] + delta * b[i - 1]))
                b.append(beta * (l[i] - l[i - 1]) + (1 - beta) * delta * b[i - 1])
                ft.append((l[i-1] + delta * b[i-1]) + s[i - season])

            elif i >= len(yt):
                h = i - len(yt) + 1 # Forecast steps
                ft.append((l[-1] + h * delta_process[h-1] * b[-1]) + s[-12:][h % season])

            else:
                l.append(alpha * (yt[i] - s[i-season]) + (1 - alpha)*(l[i - 1] + delta * b[i - 1]))
                b.append(beta * (l[i] - l[i-1]) + (1 - beta) * delta * b[i-1])
                s.append((gamma * yt[i] - (l[i-1] + delta * b[i-1])) + (1 - gamma)*s[i-season])
                ft.append((l[i - 1] + delta * b[i - 1]) + s[i - season]) # Imagine you are in t, wanting to predict t+1, thus use -1 for l, b.

        self.l = l
        self.b = b
        self.s = s

        return yt, ft

