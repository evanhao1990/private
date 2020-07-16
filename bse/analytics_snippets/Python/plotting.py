import pandas as pd
import numpy as np
import string
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib as mpl


def load_bar_data():
    q = 5
    vals = np.random.rand(q)
    labels = list(string.ascii_lowercase[:q])
    df = pd.DataFrame({'sales': vals * 1000, 'profit': vals * 0.5 * 1000,
                       'returns': vals * 0.8 * 1000}, index=labels)
    return df


def load_bar_line_data():
    """
    Example data for bar_line plot

    """

    a_17 = [100, 200, 300, 200, 100]
    a_18 = [110, 210, 360, 180, 90]
    df = pd.DataFrame({'2017': a_17, '2018': a_18}, index=['a', 'b', 'c', 'd', 'e'])
    df['yoy'] = df['2018'] / df['2017'] * 100
    return df


def format_yaxis_pct(ax):
    ax.get_yaxis().set_major_formatter(FuncFormatter('{0:.0%}'.format))


def format_xaxis_pct(ax):
    ax.get_xaxis().set_major_formatter(FuncFormatter('{0:.0%}'.format))


def format_yaxis_thousands(ax):
    ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))


def format_xaxis_thousands(ax):
    ax.get_xaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))


def twin_ax_legends(ax1, ax2):
    h, labels = ax1.get_legend_handles_labels()
    h_, labels_ = ax2.get_legend_handles_labels()
    return h + h_, labels + labels_


def get_colors():
    return mpl.rcParams['axes.prop_cycle'].by_key()['color']


def quick_bar(data, labels=None, xlabel=None, ylabel=None, title=None, y_format=None, name=None,
              legend=True, figsize=(5.4, 4.5)):
    """
    A quick bar plot

    """

    fig, ax = plt.subplots(1, 1, figsize=figsize)

    if type(data) == pd.core.frame.DataFrame:
        if len(data.columns) == 2:
            barwidth = 0.35
        if len(data.columns) == 3:
            barwidth = 0.25
        if len(data.columns) == 4:
            barwidth = 0.15
		
        for i, col in enumerate(data.columns):
            vals = data[col]

            if i == 0:
                ind = np.arange(len(vals))
            else:
                ind = ind + barwidth

            ax.bar(ind, vals, width=barwidth, label=vals.name)
        
        ind = np.arange(len(data))
        pos = ind + barwidth*(len(data.columns) - 1)/2
        ax.set_xticks(pos)

    if type(data) == pd.core.series.Series:
        barwidth = 0.75
        ind = np.arange(len(data))
        ax.bar(ind, data, width=barwidth, label=data.name)

    if type(data) == list:
        barwidth = 0.75
        ind = np.arange(len(data))
        if name:
            l = name
        else:
            l = 'na'
        ax.bar(ind, data, width=barwidth, label=l)
        ax.set_xticks(ind)

    if y_format:
        if y_format == 'pct':
            format_yaxis_pct(ax)
        if y_format == 'thousands':
            format_yaxis_thousands(ax)
        if y_format == 'millions':
            pass

    if labels:
        xtl = ax.set_xticklabels(labels)
    if xlabel:
        xl = ax.set_xlabel(xlabel)
    if ylabel:
        yl = ax.set_ylabel(ylabel)
    if title:
        t = ax.set_title(title)
    if legend:
        l = ax.legend()

    return fig, ax


def quick_bar_line(data, labels=None, xlabel=None, ylabel=None, ax2_ylabel=None, title=None,
                   y_format=None, name=None, legend=True, figsize=(5.4, 4.5), ax2_ylim=None,
                   add_index_labels=True):
    """
    Plot a bar showing absolute values, and a line showing the index between them.

    For example: GSII 2017 vs GSII 2018 + index.

    Returns:
        fig, (ax, ax2) for further processing.

    Parameters
    -----------

    data: dataframe with the following structure:
        col1, col2, col3 where (col1, col2) are the absolute values, and col3 is the yoy index
        See bar_line_data() for example.

    labels: list
        The xticklabels you want

    xlabel, ylabel, ax2_ylabel, title: string
        Will set the corresponding labels on the plot

    y_format: ['pct', 'thousands', 'millions']
        which format is the y axis on ax.

    name: string
        If you want to overwrite the name of the series in the bar plots.

    add_index_labels: Bool
        Will add index labels to the index markers

    ax2_ylim: tuple (bottom, top)
        set the y limits on the ax2 axis.

    """

    # Get data
    df_abs = data.iloc[:, :2]
    df_yoy = data.iloc[:, -1]

    fig, ax = quick_bar(df_abs, labels=labels, xlabel=xlabel, ylabel=ylabel, title=title,
                        y_format=y_format, name=name, legend=legend, figsize=figsize)

    ax2 = ax.twinx()
    ind = np.arange(len(df_yoy))
    vals = df_yoy.values.tolist()
    center_offset = 0.175
    ax2.plot(ind + center_offset, vals, marker='o', color='#5B5B5B', label='index',
             linestyle='None', markersize=7)

    # Set limits
    if ax2_ylim:
        ax2.set_ylim(bottom=ax2_ylim[0], top=ax2_ylim[1])
    # Set axhline
    ax2.axhline(100, c='k', ls='--')
    ax2.grid(False)

    # Formatting
    if ax2_ylabel:
        ax2.set_ylabel(ax2_ylabel)

    # Legends
    h, l = twin_ax_legends(ax, ax2)
    ax.legend(h, l)

    # Add index labels
    if add_index_labels:
        for i, j in enumerate(df_yoy):
            ax2.annotate(str(int(np.round(j, 0))), xy=(i + center_offset, j * 1.02))

    return fig, (ax, ax2)
