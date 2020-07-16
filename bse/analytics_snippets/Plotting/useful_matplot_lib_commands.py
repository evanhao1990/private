## Get colors used in template style
import matplotlib as mpl
print mpl.rcParams['axes.prop_cycle']

## Get colors used in template style
def get_colors():
    import matplotlib as mpl
    return mpl.rcParams['axes.prop_cycle'].by_key()['color']
	

color_map = ['#4C72B0', '#55A868', '#C44E52', '#8172B2', '#CCB974', '#64B5CD']
ind = np.arange(0, len(color_map))

fig, ax = plt.subplots(1, 1, figsize=(8.4, 4.5))
for i, c in zip(ind, color_map):
    ax.plot(np.arange(10) + i, label=c)
ax.legend()


## Thousands formatting on y/x axis
from matplotlib.ticker import FuncFormatter

def format_yaxis(ax):
    ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))

def format_xaxis(ax):
    ax.get_xaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
	

## Set fontsizes for all text
for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels()):
	item.set_fontsize(8)
	
	
## Set the major tick locator (useful for dates)
xloc = plt.MaxNLocator(6)
ax.xaxis.set_major_locator(xloc)


## Set the color of an individual bar on barplot (assumes dataframe index, is same index used to make barplot)
pos = group.index.get_loc(0) # Zero can be anything (perhaps it is a label, or a date or a number)
ax.patches[pos].set_facecolor('#9B9B9B')
