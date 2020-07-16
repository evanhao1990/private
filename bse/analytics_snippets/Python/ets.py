"""
Implementation of ETS models with scipy optimization.

References:
    Forecasting with Exponential Smoothing: State Space approach

By Rory Vigus 2018-04-06

"""

import pandas as pd
import numpy as np
from scipy.optimize import fmin_l_bfgs_b


class ETS(object):
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


    def load_test_data(self, type='simple'):
        """Loads some test data in the form of a series with date index.

        """

        if type == 'complex':

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

        if type == 'simple':

            yt = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 5000, 4000, 3000, 2500, 2000, 1500, 2500,
                  3500,
                  4500, 5500, 6500, 7500, 5500, 4500, 3500, 3000, 2500, 2000, 3000, 4000, 5000, 6000,
                  7000, 8000,
                  6000, 5000, 4000, 3500, 3000, 2500]


            return yt


    def alignment(self, yt, prediction_interval):
        """Align lengths of prediction interval lists to yt"""

        nan_list = [np.nan for i in range(len(yt))]
        pred_int_upper = nan_list + prediction_interval[0]
        pred_int_lower = nan_list + prediction_interval[1]

        return (pred_int_upper, pred_int_lower)


    def sse(self, params, *args):
        """Sum of squared error."""

        yt = args[0]
        mtype = args[1]
        season = args[2]
        sse = 0

        if mtype == 'AAA':

            alpha, beta, gamma = params

            # Calculate initial level
            l = [sum(yt[:season]) / float(season)]

            # Calculate initial trend
            b = [(sum(yt[season:season * 2]) - sum(yt[:season])) / season ** 2]

            # Calculate initial season
            s = [yt[i] - l[0] for i in range(season)]

            # Initial error
            e = [np.nan]

            # Initial forecast
            f = [np.nan]

            for i in range(1, len(yt)):

                if i < season:  # we already have 12 seasonal values
                    l.append(l[0])
                    b.append(b[0])
                    e.append(np.nan)
                    f.append(np.nan)

                else:
                    f.append(l[i - 1] + b[i - 1] + s[i - season])
                    e.append(yt[i] - f[i])
                    l.append(l[i - 1] + b[i - 1] + (alpha * e[i]))
                    b.append(b[i - 1] + (beta * e[i]))
                    s.append(s[i - season] + (gamma * e[i]))

                    sse += (yt[i] - f[i]) ** 2

        if mtype == 'MAM':

            alpha, beta, gamma = params

            # Calculate initial level
            l = [sum(yt[:season]) / float(season)]

            # Calculate initial trend
            b = [(sum(yt[season:season * 2]) - sum(yt[:season])) / season ** 2]

            # Calculate initial season
            s = [yt[i] / l[0] for i in range(season)]

            # Initial error
            e = [np.nan]

            # Initial forecast
            f = [np.nan]

            for i in range(1, len(yt)):

                if i < season:  # we initialize at t=12, therefore everything before is nan.
                    l.append(l[0])
                    b.append(b[0])
                    e.append(np.nan)
                    f.append(np.nan)

                else:
                    f.append((l[i - 1] + b[i - 1]) * s[i - season])
                    e.append((yt[i] - f[i]) / f[i])
                    l.append((l[i - 1] + b[i - 1]) * (1 + alpha * e[i]))
                    b.append(b[i - 1] + beta * (l[i - 1] + b[i - 1]) * e[i])
                    s.append(s[i - season] * (1 + gamma * e[i]))

                    sse += (yt[i] - f[i]) ** 2

        return sse


    def negll(self, params, *args):
        """Log likelhood."""

        yt = args[0]
        mtype = args[1]
        season = args[2]

        if mtype == 'AAA':

            alpha, beta, gamma = params

            # Calculate initial level
            l = [sum(yt[:season]) / float(season)]

            # Calculate initial trend
            b = [(sum(yt[season:season * 2]) - sum(yt[:season])) / season ** 2]

            # Calculate initial season
            s = [yt[i] - l[0] for i in range(season)]

            # Initial error
            e = [np.nan]

            # Initial forecast
            f = [np.nan]

            for i in range(1, len(yt)):

                if i < season:  # we already have 12 seasonal values
                    l.append(l[0])
                    b.append(b[0])
                    e.append(np.nan)
                    f.append(np.nan)

                else:
                    f.append(l[i - 1] + b[i - 1] + s[i - season])
                    e.append(yt[i] - f[i])
                    l.append(l[i - 1] + b[i - 1] + (alpha * e[i]))
                    b.append(b[i - 1] + (beta * e[i]))
                    s.append(s[i - season] + (gamma * e[i]))

            # likelihood calculations
            # Ignore first m seasonal points
            e_1 = e[12:]
            f_1 = e[12:]
            n = len(yt) - season
            sigma = np.std(e_1)
            rxt = 1  # r(xt-1) = 1 (additive error)

            log_rxt = [np.log(abs(rxt)) for i in range(n)]
            e_over_sigma2 = [((e_1[i]**2) / (sigma**2)) for i in range(n)]

            l = (-n/2)*(np.log(2*np.pi*(sigma**2))) - sum(log_rxt) - 0.5 * sum(e_over_sigma2)

            return -l

        if mtype == 'MAM':


            alpha, beta, gamma = params

            # Calculate initial level
            l = [sum(yt[:season]) / float(season)]

            # Calculate initial trend
            b = [(sum(yt[season:season * 2]) - sum(yt[:season])) / season ** 2]

            # Calculate initial season
            s = [yt[i] / l[0] for i in range(season)]

            # Initial error
            e = [np.nan]

            # Initial forecast
            f = [np.nan]

            for i in range(1, len(yt)):

                if i < season:  # we initialize at t=12, therefore everything before is nan.
                    l.append(l[0])
                    b.append(b[0])
                    e.append(np.nan)
                    f.append(np.nan)

                else:
                    f.append((l[i - 1] + b[i - 1]) * s[i - season])
                    e.append((yt[i] - f[i]) / f[i])
                    l.append((l[i - 1] + b[i - 1]) * (1 + alpha * e[i]))
                    b.append(b[i - 1] + beta * (l[i - 1] + b[i - 1]) * e[i])
                    s.append(s[i - season] * (1 + gamma * e[i]))

            # likelihood calculations
            # Ignore first m seasonal points
            e_1 = e[12:]
            f_1 = e[12:]
            n = len(yt) - season
            sigma = np.std(e_1)

            # r(xt-1) = u_t (multiplicative error)

            log_rxt = [np.log(abs(f_1[i])) for i in range(n)]
            e_over_sigma2 = [((e_1[i] ** 2) / (sigma ** 2)) for i in range(n)]

            l = (-n / 2) * (np.log(2 * np.pi * (sigma ** 2))) - sum(log_rxt) - 0.5 * sum(e_over_sigma2)

            return -l


    def ETS_AAA(self, yt, season, horizon, alpha=None, beta=None, gamma=None, method='ll'):
        """ETS model with Additive error, Additive Trend, Additive seasonality"""


        if (alpha == None or beta == None or gamma == None):
            initial_values = np.array([0.1, 0.01, 0.01])
            boundaries = [(0, 1), (0, 1), (0, 1)]
            mtype = 'AAA'


            if method == 'll':
                parameters = fmin_l_bfgs_b(self.negll, x0=initial_values, args=(yt, mtype, season),
                                           bounds=boundaries, approx_grad=True)

            else:
                parameters = fmin_l_bfgs_b(self.sse, x0=initial_values, args=(yt, mtype, season),
                                           bounds=boundaries, approx_grad=True)


            alpha, beta, gamma = parameters[0]
            print parameters[0]
            self.params = parameters

        # Calculate initial level
        l = [sum(yt[:season]) / float(season)]

        # Calculate initial trend
        b = [(sum(yt[season:season*2]) - sum(yt[:season])) / season ** 2]

        # Calculate initial season
        s = [yt[i] - l[0] for i in range(season)]

        # Initial error
        e = [np.nan]

        # Initial forecast
        f = [np.nan]

        for i in range(1, len(yt) + horizon):

            if i < season: # we initialize at t=12, therefore everything before is nan.
                l.append(l[0])
                b.append(b[0])
                e.append(np.nan)
                f.append(np.nan)

            elif i >= len(yt):
                h = i - len(yt) + 1
                f.append(l[-1] + h * (b[-1]) + s[-12:][h%season])

            else:
                f.append(l[i-1] + b[i-1] + s[i-season])
                e.append(yt[i] - f[i])
                l.append(l[i-1] + b[i-1] + (alpha * e[i]))
                b.append(b[i-1] + (beta * e[i]))
                s.append(s[i-season] + (gamma * e[i]))


        # Prediction interval calculations
        e_1 = e[season:]
        std = np.std(e_1)

        f_1 = f[-horizon:]
        u = []
        cj_sq = [alpha ** 2]
        v = [std ** 2]
        dj = [0]

        pred_int_upper = [f_1[0] + 1.96 * std]
        pred_int_lower = [f_1[0] - 1.96 * std]

        for i in range(1, horizon):
            dj.append((i - 1) / season)
            cj_sq.append((alpha + (beta*i) + dj[i])**2)
            v.append((std ** 2) * (1 + np.sum(cj_sq[:i])))
            pred_int_upper.append(f_1[i] + 1.96 * (v[i]**0.5))
            pred_int_lower.append(f_1[i] - 1.96 * (v[i]**0.5))

        self.l = l
        self.b = b
        self.s = s
        self.f = f
        self.e = e

        prediction_intervals = self.alignment(yt, (pred_int_upper, pred_int_lower))

        return f, prediction_intervals


    def ETS_MAM(self, yt, season, horizon, alpha=None, beta=None, gamma=None, method='ll'):

        if (alpha == None or beta == None or gamma == None):
            initial_values = np.array([0.1, 0.01, 0.01])
            boundaries = [(0.0, 1.0), (0.0, 1.0), (0.0, 1.0)]
            mtype = 'MAM'


            if method == 'll':
                parameters = fmin_l_bfgs_b(self.negll, x0=initial_values, args=(yt, mtype, season),
                                           bounds=boundaries, approx_grad=True)

            else:
                parameters = fmin_l_bfgs_b(self.sse, x0=initial_values, args=(yt, mtype, season),
                                           bounds=boundaries, approx_grad=True)


            alpha, beta, gamma = parameters[0]
            print parameters[0]
            self.params = parameters

        # Calculate initial level
        l = [sum(yt[:season]) / float(season)]

        # Calculate initial trend
        b = [(sum(yt[season:season*2]) - sum(yt[:season])) / season ** 2]

        # Calculate initial season
        s = [yt[i] / l[0] for i in range(season)]

        # Initial error
        e = [np.nan]

        # Initial forecast
        f = [np.nan]

        for i in range(1, len(yt) + horizon):

            if i < season: # we initialize at t=12, therefore everything before is nan.
                l.append(l[0])
                b.append(b[0])
                e.append(np.nan)
                f.append(np.nan)

            elif i >= len(yt):
                h = i - len(yt) + 1
                f.append((l[-1] + (h * b[-1])) * s[-12:][h%season])

            else:
                f.append((l[i-1] + b[i-1]) * s[i-season])
                e.append((yt[i] - f[i]) / f[i])
                l.append((l[i-1] + b[i-1]) * (1 + alpha * e[i]))
                b.append(b[i-1] + beta * (l[i-1] + b[i-1]) * e[i])
                s.append(s[i-season] * (1 + (gamma * e[i])))

        # Prediction interval calculations
        e_1 = e[season:]
        sigma = np.std(e_1)
        s_1 = s[-season:]
        f_1 = f[-horizon:]

        u = []
        u_sq = []
        cj_sq = []
        theta = []
        sum_theta = []
        v = []

        pred_int_upper = []
        pred_int_lower = []

        for i in range(0, horizon):

            u.append(f_1[i] / s_1[i % season])
            u_sq.append(u[i] ** 2)
            cj_sq.append((alpha + (i * beta)) ** 2)

            if i == 0:
                theta.append(u_sq[i])
                sum_theta.append(0)
            else:
                sum_theta.append(sum_theta[i-1] + (theta[i-1] * cj_sq[i-1]))
                theta.append(u_sq[i] + ((sigma**2) * sum_theta[i]))

            v.append((s_1[i % season] ** 2) * (theta[i] * ((1 + (sigma ** 2)) * ((1 + ((gamma ** 2) * (sigma ** 2))) ** np.floor((i + 1) / season))) - u_sq[i]))

            pred_int_upper.append(f_1[i] + 1.96 * (v[i]**0.5))
            pred_int_lower.append(f_1[i] - 1.96 * (v[i]**0.5))

        self.l = l
        self.b = b
        self.s = s
        self.f = f
        self.e = e

        prediction_intervals = self.alignment(yt, (pred_int_upper, pred_int_lower))

        return f, prediction_intervals