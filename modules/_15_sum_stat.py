"""

    """

import pandas as pd

from main import *

sum_stat = Path('sum_stat/')
tables = Path('tables/')

cd = tse_ns.DIndInsCols()

def add_second_zero(df) :
    for cl in df.columns :
        msk = df[cl].str.fullmatch(r'\d+\.\d')
        df.loc[msk , cl] = df.loc[msk , cl] + '0'
    return df

def make_decimal_point_slash(df) :
    df = df.applymap(lambda x : x.replace('.' , '/'))
    return df

def tables_fa(fp , outfn) :
    df = pd.read_csv(fp , sep = None)
    df = df.fillna('')
    df = make_decimal_point_slash(df)
    df = df[reversed(df.columns)]
    df.to_html(tables / f'{outfn}.html' , index = False)

##
def days_fa() :
    pass

    ##
    # read the main data
    df = pd.read_csv(fpn.main)

    ##
    # get years and trading days
    df1 = df[[c.jd , c.jyr]]
    df1 = df1.drop_duplicates()
    df1 = df1.groupby(c.jyr).count()
    df1 = df1.reset_index()

    ##
    # get number of firms each year
    df2 = df[[c.ftic , c.jyr]].drop_duplicates()
    df2 = df2.groupby(c.jyr).count()
    df2 = df2.reset_index()

    ##
    # merge the two
    dfo = df1.merge(df2 , on = [c.jyr])

    ##
    dfo = dfo.astype(str)

    ##
    dfo.to_csv(sum_stat / 'days.csv' , index = False)

def val_fa() :
    pass

    ##
    df = pd.read_csv(fpn.main)

    ##
    df1 = df[[cd.bsv , cd.ssv , cd.bdv , cd.sdv , c.jyr]]
    df1 = df1.groupby(c.jyr).sum() / 10 ** 9

    ##
    df2 = df[[cd.bsva , cd.ssva , cd.bdva , cd.sdva , c.jyr]]
    df2 = df2.groupby(c.jyr).sum() / 10 ** 13

    ##
    dfo = df1.join(df2)

    ##
    dfo = dfo.round(2)
    dfo = dfo.astype(str)

    ##
    dfo = add_second_zero(dfo)

    ##
    dfo = make_decimal_point_slash(dfo)

    ##
    dfo = dfo.reset_index()

    ##
    dfo = dfo.astype(str)

    ##
    dfo.to_csv(sum_stat / 'val.csv' , index = False)

def count_fa() :
    pass

    ##
    df = pd.read_csv(fpn.main)

    ##
    dfc = df[[cd.bsc , cd.ssc , cd.bdc , cd.sdc , c.jyr]]

    ##
    df1 = dfc.groupby(c.jyr).mean()
    df1 = df1.rename(columns = lambda x : x + '_mean')

    ##
    df2 = dfc.mean().to_frame().T.drop(columns = [c.jyr])
    df2 = df2.rename(columns = lambda x : x + '_mean')
    df2.index = ['all']

    ##
    df3 = pd.concat([df1 , df2] , axis = 0)
    df3 = df3.reset_index()
    df3 = df3.rename(columns = {
            'index' : c.jyr
            })
    df3 = df3.set_index(c.jyr)

    ##
    df1 = dfc.groupby(c.jyr).std()
    df1 = df1.rename(columns = lambda x : x + '_std')

    ##
    df2 = dfc.std().to_frame().T.drop(columns = [c.jyr])
    df2 = df2.rename(columns = lambda x : x + '_std')
    df2.index = ['all']

    ##
    df4 = pd.concat([df1 , df2] , axis = 0)
    df4 = df4.reset_index()
    df4 = df4.rename(columns = {
            'index' : c.jyr
            })
    df4 = df4.set_index(c.jyr)

    ##
    dfo = df3.join(df4)

    ##
    dfo = dfo.round(2)
    dfo = dfo.astype(str)

    ##
    dfo = add_second_zero(dfo)

    ##
    dfo = make_decimal_point_slash(dfo)

    ##
    dfo = dfo.reset_index()

    ##
    dfo = dfo.astype(str)

    ##
    cols_ord = {
            cd.sdc + '_std'  : None ,
            cd.bdc + '_std'  : None ,
            cd.ssc + '_std'  : None ,
            cd.bsc + '_std'  : None ,
            cd.sdc + '_mean' : None ,
            cd.bdc + '_mean' : None ,
            cd.ssc + '_mean' : None ,
            cd.bsc + '_mean' : None ,
            c.jyr            : None
            }

    dfo = dfo[cols_ord.keys()]

    ##
    dfo.to_html(sum_stat / 'count.html' , index = False)

def var_fa() :
    pass

    ##
    df = pd.read_csv(fpn.main)

    ##
    cols = {
            cn.sti0 : None ,
            cn.dti0 : None ,
            'r1'    : None ,
            'r2'    : None ,
            'r6'    : None ,
            'r28'   : None ,
            cn.sci0 : None ,
            cn.dci0 : None ,
            }

    df = df[cols.keys()]

    ##
    dfo = df.describe()

    ##
    for cl in [cn.r1 , cn.r2 , cn.r6 , cn.r28] :
        dfo[cl] = dfo[cl] * 100

    ##
    dfo = dfo.T

    ##
    dfo['count'] = dfo['count'].astype(int).astype(str)

    ##
    dfo = dfo.round(2)
    dfo = dfo.astype(str)

    ##
    dfo = add_second_zero(dfo)

    ##
    dfo = make_decimal_point_slash(dfo)

    ##
    locs = [cn.r1 , cn.r2 , cn.r6 , cn.r28]
    cols = ['mean' , 'std' , 'min' , '25%' , '50%' , '75%' , 'max']

    for col in cols :
        dfo.loc[locs , col] = dfo.loc[locs , col] + '%'

    ##
    dfo.loc[locs , 'count'] = dfo.loc[locs , 'count'].astype(int) / 100
    dfo.loc[locs , 'count'] = dfo.loc[locs , 'count'].astype(int).astype(str)

    ##
    dfo = dfo.reset_index()

    ##
    cord = {
            'max'   : None ,
            '75%'   : None ,
            '50%'   : None ,
            '25%'   : None ,
            'min'   : None ,
            'std'   : None ,
            'mean'  : None ,
            'count' : None ,
            'index' : None
            }

    dfo = dfo[cord.keys()]

    ##
    dfo.to_html(sum_stat / 'var.html' , index = False)

##
def make_all_tables_fa() :
    pass

    ##
    fps = {
            '/Users/mahdi/Dropbox/1-GitHub/TeIAS-Thesis-STATA/c_Tables/v6/main/main_short.txt'       : 'main' ,
            '/Users/mahdi/Dropbox/1-GitHub/TeIAS-Thesis-STATA/c_Tables/v6/ci/ci_short.txt'           : 'ci' ,
            '/Users/mahdi/Dropbox/1-GitHub/TeIAS-Thesis-STATA/c_Tables/v6/size/size_short.txt'       : 'size' ,
            '/Users/mahdi/Dropbox/1-GitHub/TeIAS-Thesis-STATA/c_Tables/v6/no_news/no_news_short.txt' : 'nonews'
            }

    for fpi , fpo in fps.items() :
        tables_fa(fpi , fpo)

    ##
    list(reversed(df.columns))
