import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy.optimize import brute
import sys


"""



ARIMA model
------------

This file contains implementations of statsmodels ARIMA & SARIMA models.
This class effectively implements R's auto-arima() method. (Determines optimal ARIMA models automatically)
Can be used on ARMA, ARIMA and SARIMA models.

Created:    2017-05-31
By:         Rory Vigus


Model Descriptions
------------------

    Non-seasonal models (ARIMA, ARIMAX)
        Standard ARMA family of models.
        See: https://www.otexts.org/fpp/8

        Notation: ARIMA(p, d, q)
         p = AR terms (Autoregressive)
         d = differencing (integration)
         q = MA terms (Moving Average)


    Seasonal models (SARIMA, SARIMAX)
        Standard SARIMA family of models
        See: https://www.otexts.org/fpp/8/9

        Notation: SARIMA(p, d, q)(P, D, Q, M)
        p, d, q (see above)

        P = Seasonal AR terms
        D = Seasonal differencing
        Q = Seasonal MA terms
        M = Seasonality of a series


How to use:
------------

    Class ARIMA() is effectively a wrapper around the standard ARIMA class from statsmodels.
    Statsmodels doesn't provide an implementation in order to automatically determine optimal lag lengths for models. Any
    models estimated using the standard library have to be specified each time by the user.

    This class aims to automate the model selection process, but also retains the ability to self-determine model orders.

    Example usage: (Seasonal Model)
        s_mod = ARIMA(yt=yt, mtype='S', pmax=2, qmax=2, s_pmax=1, s_qmax=1, seasonality=12).fit()
        print s_mod.predict(start=len(yt), end=len(yt) + 12, dynamic=True)

    Example usage: (Non-seasonal Model)
        ns_mod = ARIMA(yt=yt, mtype='NS', pmax=2, qmax=2).fit()
        print ns_mod.forecast(steps=12)[0] # For non-seasonal models use forecast, not predict.


Parameters:
-----------

    Upon initialization:
        yt : series with datetime index (YYYY-MM-DD)
            Time series data

        exog : series with datetime index (YYYY-MM-DD)
            Time series data

        mtype : string
            Which model type to use: NS = Non-seasonal
                                      S = Seasonal

        pmax : int
            AR(p) max lag length

        qmax : int
            MA(q) max lag length

        s_pmax : int
            Seasonal AR(P) max lag length

        s_qmax : int
            Seasonal MA(Q) max lag length

        seasonality : int
            Seasonality of the series: monthly = 12, quarterly = 4 etc.

        set_ns_order : tuple
            Manually specify the p, d, q parameters of a non-seasonal model

        set_s_order : tuple
            Manually specify the P, D, Q parameters of a seasonal model

        ns_d : int
            placeholder for the non-seasonal difference - this is not a returned parameter of the results class.
            Thus we save it ourselves.

        aic_list : list of tuples
            if mtype == 'S':
                (ns_order, s_order, aic)
            if mtype == 'NS':
                (ns_order, aic)

            Use this as a diagnostic to compare aic values between model orders.

    .fit() method:
        Call this method in order to fit an auto-arima() model under the given constaints,
        or a self-specified model.

        if Non-seasonal ARIMA:
            returns: ARIMAResults class
            http://www.statsmodels.org/0.8.0/generated/statsmodels.tsa.arima_model.ARIMAResults.html#statsmodels.tsa.arima_model.ARIMAResults

        if Seasonal ARIMA:
            returns: SARIMAXResults class
            http://www.statsmodels.org/stable/statespace.html#seasonal-autoregressive-integrated-moving-average-with-exogenous-regressors-sarimax


        NOTE!
        Pretty much everything that can be returned from the model (i.e optimal params and so on) can be accessed from within the
        results class. Check the documentation for a full list of attributes/methods.


"""

class ARIMA(object):

    def __init__(self, yt, exog=None, mtype=None, pmax=None, qmax=None, s_pmax=None, s_qmax=None, seasonality=None,
                 set_ns_order=(None, None, None), set_s_order=(None, None, None)):
        self.yt = yt
        self.exog=exog
        self.mtype = mtype
        self.pmax = pmax
        self.qmax = qmax
        self.s_pmax = s_pmax
        self.s_qmax = s_qmax
        self.seasonality = seasonality
        self.set_ns_order = set_ns_order
        self.set_s_order = set_s_order
        self.ns_d = None
        self.aic_list = []
        self.opt_params = None

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


    def _adf_test(self, yt):
        """ Performs an Augmented Dickey Fuller test for stationarity

            If the test statistic is not more negative than the critical values at 5%, then the series should
            be differenced.

        """

        adf = sm.tsa.stattools.adfuller(yt, autolag='AIC')
        output = pd.Series(adf[:4], index=['t-stat', 'p', 'lags', 'obs'])

        for key, value in adf[4].items():
            output['sig-%s' % key] = value

        return output


    def _determine_series_integration(self):
        """ Determines the order of non-seasonal differencing
            Upper bounds of d should be 2 (said my econometrics professor - hard to find references on this though)
            If it is more than 2, then you should transform the data (take logs perhaps?). Currently it is not possible to do this.
            We risk then that on rare occasion a series is not stationary after d=2, then we are forecasting on a non-stationary series.

        """

        adfresult = self._adf_test(self.yt)
        stationary = False
        d = 0
        yt = self.yt

        while stationary is False:
            if d < 2:
                if adfresult['t-stat'] > adfresult['sig-5%']:
                    ydiff = (yt - yt.shift(1)).dropna()
                    adfresult = self._adf_test(ydiff)
                    yt = ydiff
                    d += 1
                else:
                    stationary = True
            else:
                d = 2
                stationary = True

        self.ns_d = d

        return d


    def _aic_minimize(self, order, endog, exog):
        """ Find optimal model order by selecting the model that minimizes the aic.

            In the case of model failure (common when testing all combinations of model orders) aic is assigned the value
            of 10000 ** 3. AIC values of successful models will always be smaller than this.

            I first assigned np.nan as this value but this caused brute to think that the best fitting model was one
            where the aic was np.nan. This subsequently broke the rest of the script.

        """

        if self.mtype == 'NS':
            ns_order = order[:3]
            s_order = (0, 0, 0, 0)

            aic = 10000 ** 3
            try:
                fit = sm.tsa.statespace.SARIMAX(endog=endog, exog=exog, order=ns_order, seasonal_order=s_order,
                                                enforce_stationarity=True, enforce_invertibility=True).fit(disp=0)
                aic = fit.aic

                if np.isnan(aic):
                    aic = 10000.00 ** 3

            except: pass
            self.aic_list.append((ns_order, aic))

        elif self.mtype == 'S':
            ns_order = order[:3]
            s_order = order[3:]
            s_order = np.append(s_order, [self.seasonality])

            aic = 10000.00 ** 3
            try:
                fit = sm.tsa.statespace.SARIMAX(endog=endog, exog=exog, order=ns_order, seasonal_order=s_order,
                                                enforce_stationarity=True, enforce_invertibility=True).fit(disp=0)
                aic = fit.aic

                if np.isnan(aic):   # Catches additional errors where model may fit - but aic calculation returns nan
                    aic = 10000.00 ** 3

            except: pass
            self.aic_list.append((ns_order, s_order, aic))

        else:
            print 'Model type not recognized'
            sys.exit()

        return aic


    def _optimize_params(self):
        """ Brute force algorithm:
            1) Fit all possible models as allowed by the search space grid.
            2) Choose the model with that minimizes AIC.

        """

        # Determine non-seasonal differencing - seasonal differencing is determined automatically later.
        d = self._determine_series_integration()

        if self.mtype == 'NS':
            # set the search grid & attack
            grid = (slice(0, self.pmax + 1, 1), slice(d, d+1, 1), slice(0, self.qmax + 1, 1))
            opt_params = brute(self._aic_minimize, grid, args=(self.yt, self.exog), finish=None)     # args=(endog, exog)

        elif self.mtype == 'S':
            grid = (
                slice(0, self.pmax + 1, 1), slice(d, d+1, 1), slice(0, self.qmax + 1, 1),
                slice(0, self.s_pmax + 1, 1), slice(0, 2, 1), slice(0, self.s_qmax + 1, 1)
            )

            opt_params = brute(self._aic_minimize, grid, args=(self.yt, self.exog), finish=None)

        else:
            print 'Model type not recognized'
            sys.exit()

        self.opt_params = opt_params

        return opt_params


    def fit(self):
        """ Use the optimal parameters to fit a model."""

        # Determine non-seasonal differencing - seasonal differencing is determined automatically later.
        d = self._determine_series_integration()

        if (self.pmax != None or self.qmax != None or self.s_pmax!=None or self.s_qmax!=None)\
                and not all(self.set_ns_order) is False or not all(self.set_s_order) is False:
            # Not all(set_order) checks whether all are None
            # If both parameters are set then sys.exit()

            print "pmax & qmax cannot have values if you provide you own model orders"
            sys.exit()

        elif not all(self.set_ns_order) and not all(self.set_s_order):
            params = self._optimize_params()

            if self.mtype == 'NS':
                p, d, q = int(params[0]), int(params[1]), int(params[2])
                P, D, Q, S = (0, 0, 0, 0)
            else:
                p, d, q = int(params[0]), int(params[1]), int(params[2])
                P, D, Q, S = int(params[3]), int(params[4]), int(params[5]), self.seasonality

            mod = sm.tsa.SARIMAX(endog=self.yt, exog=self.exog, order=(p, d, q),
                                 seasonal_order=(P, D, Q, S))
            res = mod.fit(disp=0)

        else:

            if self.mtype == 'NS':
                p, d, q = self.set_ns_order
                P, D, Q, S = (0, 0, 0, 0)
            else:
                p, d, q = self.set_ns_order
                P, D, Q, S = self.set_s_order

            mod = sm.tsa.SARIMAX(endog=self.yt, exog=self.exog, order=(p, d, q),
                                 seasonal_order=(P, D, Q, S))
            res = mod.fit(disp=0)


        return res