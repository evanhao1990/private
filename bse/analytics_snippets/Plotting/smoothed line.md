

```python
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from scipy.interpolate import interp1d
```


```python
def exp(n):
    return np.exp(n)
```


```python
# prepare data
data = np.arange(0,10)
data = exp(data)
df = pd.DataFrame(data, columns={'exp'})
```


```python
# normal line
df.plot()
plt.show()
```


![png](smoothed%20line_files/smoothed%20line_3_0.png)



```python
# smoothed line
x = np.arange(len(df))
xnew = np.linspace(x.min(), x.max(), 300)
smooth = interp1d(x,df['exp'], kind='quadratic')
plt.plot(xnew, smooth(xnew), label='exp')
plt.legend()
plt.show()
```


![png](smoothed%20line_files/smoothed%20line_4_0.png)

