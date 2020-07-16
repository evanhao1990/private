
## Regression plots


```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import os
import statsmodels.api as sm
import seaborn as sns

%matplotlib inline
plt.style.use('bsestyle_color')

def get_colors():
    return mpl.rcParams['axes.prop_cycle'].by_key()['color']

colors = get_colors()
print colors
```

    [u'#4c72b0', u'#55a868', u'#c44e52', u'#8172b2', u'#ccb974', u'#64b5cd']
    

### Higher-order regression plot with population line


```python
x = np.random.normal(size=100)
eps = np.random.normal(loc=0, scale=0.25, size=100)
y = -1 + 0.5*x + 0.3*x**2 - 0.2*x**3 + eps

fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
sns.regplot(x=x, y=y, order=3, scatter_kws={'alpha':0.5, 'edgecolor':'w'}, ax=ax, label='Sample')

# Population line
f = lambda x: -1 + 0.5*x + 0.3*x**2 - 0.2*x**3
x_p = np.linspace(x.min(), x.max(), num=100)
ax.plot(x_p, f(x_p), label='Population')

ax.set_ylabel('y')
ax.set_xlabel('x')
ax.legend()
plt.tight_layout()
```


![png](Regression%20Plots_files/Regression%20Plots_3_0.png)


### Simple linear regression


```python
x = np.random.normal(size=100)
eps = np.random.normal(loc=0, scale=0.25, size=100)
y = 2 + 0.5*x + eps

fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
sns.regplot(x=x, y=y, order=1, scatter_kws={'alpha':0.5, 'edgecolor':'w'}, ax=ax, label='Sample')

# Population line
f = lambda x: 2 + 0.5*x
x_p = np.linspace(x.min(), x.max(), num=100)
ax.plot(x_p, f(x_p), label='Population')

ax.set_ylabel('y')
ax.set_xlabel('x')
ax.legend()
plt.tight_layout()
```


![png](Regression%20Plots_files/Regression%20Plots_5_0.png)

