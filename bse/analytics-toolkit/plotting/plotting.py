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

def calculate_growth(bar_a,bar_b,precision,format='pct'):

    """
    calculate growth in ['pct','pp','index'] 

    """
    if format=='pct':
        growth = bar_b*1.0 / bar_a - 1
        growth_label = '{:+.{}%}'.format(growth, precision)

    elif format=='pp':
        growth = bar_b - bar_a
        growth_label = '{:+.{}%}p'.format(growth, precision)

    elif format=='index':
        growth = bar_b*1.0 / bar_a * 100
        growth_label = '{:.0f}'.format(growth)

    return growth_label

def format_numbers(number,format,precision):
    """
    format numbers in ['%','M','K',',']

    """
    if format == '%':
        number_label = '{:.{}%}'.format(number, precision)

    elif format == 'M':
        number_label = '{:,.{}f}M'.format(number/1000000,precision)

    elif format == 'K':
        number_label = '{:,.{}f}K'.format(number/1000,precision)

    elif format == ',':
        number_label = '{:,.{}f}'.format(number,precision)

    return number_label

def format_y_values(ax, format='auto', precision=0):
    """
    Format y axis values
    
    Parameters
    -----------
    
    format:
        auto: automaticlly format numbers into ['%', 'K', 'M', ','] base on the value at 1/4 point of y axis (in order to avoid differect formatting on the same y axis.)
        
        '%': values shown in percent
        
        'K': values shown in thousands
        
        'M': values shown in million
        
        ',': add ',' at thousands
       
       
    precision: number of digits you want after decimal
    
    """
    
    if format == 'auto':
        
        y_max = max(abs(x) for x in ax.get_ylim())
        
        if y_max/1.05 <=1:
            ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: '{:.{}%}'.format(x, precision)))
        elif y_max/4.0 > 300000:
            ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: '{:,.1f}M'.format(x/1000000)))
        elif y_max/4.0 > 1000:  
            ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: '{:,.{}f}K'.format(x/1000,precision)))
        else:
            ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: '{:,.{}f}'.format(x,precision)))
    
    else:
        ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format_numbers(x,format=format, precision=precision)))
        

def format_x_values(ax, format, precision=0):
    ax.get_xaxis().set_major_formatter(FuncFormatter(lambda x, p: format_numbers(x, format=format, precision=precision)))


def twin_ax_legends(ax1, ax2):
    h, labels = ax1.get_legend_handles_labels()
    h_, labels_ = ax2.get_legend_handles_labels()
    return h + h_, labels + labels_


def get_colors():
    return mpl.rcParams['axes.prop_cycle'].by_key()['color']


def add_bar_labels(ax, format=None,position='top',precision=0,spacing=0.03):
    """
    Adds bar labels to top of/ in the middle of plot.

    label_format in [',', '%', 'K', 'M']

    """
    chart_height = max([abs(x) for x in ax.get_ylim()])
    
    for p in ax.patches:
        height = p.get_height()
        
        label = format_numbers(height,format=format,precision=precision)
        
        cor_x = p.get_x() + p.get_width()/2.

        if position=='center':
            cor_y = height*0.9/2.
            ax.text(cor_x, cor_y, label , ha='center', va='center',color='white',weight='bold')
            
        elif position=='top':
            cor_y = height+spacing*chart_height*(height/abs(height))
            ax.text(cor_x, cor_y, label , ha='center', va='center')



def quick_bar(data, ax, labels=None, xlabel=None, ylabel=None, title=None, y_format='auto',y_format_precision=0, name=None,
              legend=True, bar_labels=False, bar_label_format=',',bar_label_position='top',bar_label_precision=0,bar_label_spacing=0.03):
    """
    A quick bar plot

    """

    if type(data) == pd.core.frame.DataFrame:

        if len(data.columns) == 1:
            barwidth = 0.5
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
        format_y_values(ax,format=y_format,precision=y_format_precision)

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
    if bar_labels:
        add_bar_labels(ax, format=bar_label_format,position=bar_label_position,precision=bar_label_precision,spacing=bar_label_spacing)

    return ax


def quick_bar_line(data, ax, labels=None, xlabel=None, ylabel=None, ax2_ylabel=None, title=None,
                   y_format=None, name=None, legend=True, ax2_ylim=None, bar_labels=False, bar_label_format='val',
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

    quick_bar(df_abs, ax=ax, labels=labels, xlabel=xlabel, ylabel=ylabel, title=title, 
        y_format=y_format, name=name, legend=legend, bar_labels=bar_labels, bar_label_format=bar_label_format)

    ax2 = ax.twinx()
    ind = np.arange(len(df_yoy))
    vals = df_yoy.values.tolist()
    center_offset = 0.175
    ax2.plot(ind + center_offset, vals, marker='o',
             markeredgecolor='k', markerfacecolor='w',
             color='#5B5B5B', label='index',
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

    if bar_labels:
        add_bar_labels(ax, bar_label_format)

    # Add index labels
    if add_index_labels:
        for i, j in enumerate(df_yoy):
            ax2.annotate(str(int(np.round(j, 0))), xy=(i + center_offset, j * 1.02))

    return ax, ax2


def add_index_labels(ax, format='pct',precision=0,spacing=0.02):
    """
    adds index label 
    
    Parameters:
    -------------
    label_format:
        
        pct: growth in percentage numbers. (growth = y2 / y1 -1)
        
        pp : growth in percentage points. (growth = y2 - y1)
        
        index: growth in bse index numbers. (growth = y2 / y1 * 100)
    
    precision: number of digits after decimal
    
    spacing: distance to the bar
    
    """
    # total nr of bars
    n_total = len(ax.patches)
    
    # nr of bar groups
    n_groups = len(ax.get_xticks())
    
    # nr of bars per group
    n_cols = n_total / n_groups
    
    # max height of the chart
    chart_height = max([abs(x) for x in ax.get_ylim()])
    
    # starting for the second bar of the first bar group:
    for i_bar in range(n_groups, n_total):
        
        # calculate growth according to label format
        bar_a = ax.patches[i_bar-n_groups].get_height()
        bar_b = ax.patches[i_bar].get_height()
        label = calculate_growth(format=format,bar_a=bar_a,bar_b=bar_b,precision=precision)
        
        bar_height = ax.patches[i_bar].get_height()
        
        # set label x position: if 2 bars in the middle, otherise on top of the bars
        if n_cols == 2:

            cor_x = ax.patches[i_bar].get_x() 
            
            # if two bars both above/below 0, labels on top of highest bar, otherwise always on top of the positive bar.
            if bar_a * bar_b >=0:
                cor_y = (max(abs(bar_a),abs(bar_b)) + spacing*chart_height)*(bar_height/abs(bar_height))
            else:
                cor_y = (max(bar_a,bar_b) + spacing*chart_height)
                
        else:
            cor_x = ax.patches[i_bar].get_x() + ax.patches[i_bar].get_width() / 2
        
            # bar height + default 2% of the chart height
            cor_y = bar_height + spacing*chart_height*(bar_height/abs(bar_height))
        
        ax.text(cor_x,cor_y, label, ha='center', va='center') 


def quick_stacked_bar(df, segments, ax=None, hatches=None, add_bar_label=None, bar_label_format=',', bar_label_precision=0, bar_width=0.3, adjust_ylim=True):
    """
    quick stacked bar plot, with maximum support of 3 segments per bar and 3 bars per group

    Parameters
    ----------
    df: dataframe
         A 2-level index dataframe, with index level 0 as different bar groups and index level 1 as different segments.

    segments: list
         A list like ['a','b','c'], to control the sequence of the segments. The first elements in the list will be at the bottom of the bars.

        Example:

        Input dataframe:                        Output graph:

              | 1 | 2 | 3                      ^        ___ ___
        x | a | . | . | .                      |    ___|   |   |
          | b | . | . | .                      |   |_c_|   | c |
          | c | . | . | .                      |   |   | c |___|
        y | a | . | . | .                      |   |   |   |   |
          | b | . | . | .                      |   | b |___|___|
          | c | . | . | .                      |   |___| b |   |
                                               |   | a |___| a |
                                               |___|___|_a_|___|______|___|___|___|____
                                                    [1] [2] [3]        [1] [2] [3]
                                                    [    x    ]        [    y    ]
    ax: figure object

    hatches: list
              A list like [1,2,'/'], meaning the 1st(bottom) layer's second block with hatches like '/'.

        Example:

            ^        ___ ___
            |    ___|   |   |
            |   |___|   |3.3|
            |   |   |3.2|___|
            |   |2.1|   |   |
            |   |   |___|___|
            |   |___|   |   |
            |   |1.1|___|1.3|
            |___|___|___|___|______|___|___|___|____
                 [1] [2] [3]        [1] [2] [3]
                 [    x    ]        [    y    ]


    add_bar_label: string
                   'share' : showing the percentage of the bar block in total.
                   'val'   : showing the absolute value of the block

    bar_label_format: string
                      format numbers in ['%','M','K',',']

    bar_label_precision: int
                         bar label precision

    bar_width: float

    adjust_ylim: boolean
                 adjust max y to 1.2x the origin y max

    Returns
    -------
    stacked bar plot

    """

    # get default settings
    color_dic={0:['#606060','#404040','#202020'],
               1:['#bdbdbd','#969696','#737373'],
               2:['#e0e0e0','#f0f0f0','#d9d9d9']
                }
    
    font_color={0:['#404040'],
                1:['#f0f0f0'],
                2:['#f0f0f0']
                }
    hatch_dic={1:['','','',''],
               2:['','','',''],
               3:['','','','']}

    # reshape dataframe
    df = df.swaplevel()

    # pre-checks
    if ax is None:
        fig,ax = plt.subplots(1,1)

    if len(segments)>3:
        print('error: too many segements')

    if len(df.columns)>3:
        print('error: too many columns')

    # adjust default setting according to user input
    if hatches:
        for item in hatches:  
            hatch_dic[item[0]][item[1]-1] = item[2]
            
    # plotting bars
    
    # bar bottom starting from 0
    btm = df.loc[segments[0]] * 0
    
    for i in np.arange(len(segments)):
        
        # get level i dataframe
        df_i = df.loc[segments[i]]
                
        bar_width=bar_width
        
        # plot each bar starting from current btm value
        for j, col in enumerate(df_i.columns):
            
            vals = df_i[col]
            bottom = btm[col]
            
            if j == 0:
                ind = np.arange(len(vals))
            else:
                ind = ind + bar_width
                
            # set legend label as level name + column name    
            label = segments[i] + ' - ' + str(vals.name)
            ax.bar(ind, vals, width=bar_width, bottom=bottom,label=label,color=color_dic[i][j],hatch=hatch_dic[i+1][j])
            
            # add bar label in the center of each block, with share % or absolute value
            if add_bar_label:
                cor_x = ind
                cor_y = vals
                
                if add_bar_label == 'share':
                    bar_label = vals*1.0 / df[col].sum(level=1)
                elif add_bar_label == 'val':
                    bar_label = vals
                    
                for x,y,l,b in zip(cor_x,cor_y,bar_label,bottom):
                    ax.text(x,0.5*y+b, format_numbers(l,bar_label_format,bar_label_precision),ha='center', va='center',color=font_color[abs(i-2)][0],weight='bold',fontsize=11)
        
        # adding up bar bottom        
        btm = df_i + btm
    
    
    format_y_values(ax)
    
    # set xticks and xticklabels
    xticks_label = df.index.get_level_values(1).unique().tolist()
    
    xticks_pos = list(np.arange(len(xticks_label)))
    xticks_pos = [x + 0.5*bar_width*(len(df.columns)-1) for x in xticks_pos]
        
    ax.set_xticks(xticks_pos)    
    ax.set_xticklabels(xticks_label)
    
    # adjust_ylim
    if adjust_ylim:
        ax.set_ylim(0,ax.get_ylim()[-1]*1.2)
        
    plt.legend(ncol=len(df.columns))