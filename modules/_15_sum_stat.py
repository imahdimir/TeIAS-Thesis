"""

    """

from pathlib import Path

import pandas as pd
from mirutil.df import save_df_as_prq
from namespace_mahdimir import tse as tse_ns

from main import c
from main import cn
from main import fpn
from main import nws_type
from modules._13_make_main_dataset import add_news_data_to_baseline_data
from modules._13_make_main_dataset import change_adjusted_returns_col_names
from modules._13_make_main_dataset import \
    get_market_adjusted_returns_keep_relevant_cols
from modules._13_make_main_dataset import get_measures_data
from modules._13_make_main_dataset import read_news_data_keep_revelant_cols
from modules._13_make_main_dataset import read_refrence_points_data
from modules._13_make_main_dataset import read_weekday_data
from modules._13_make_main_dataset import gen_jyear

cl = tse_ns.DAllCodalLetters()
cd = tse_ns.DIndInsCols()

def keep_related_cols_for_variation_analysis(df) :
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

    return df[cols.keys()]

def main() :
    pass

    ##
    df = pd.read_csv(fpn.main)

    ##
    df = keep_related_cols_for_variation_analysis(df)

    ##
    df1 = df.describe().T

    ##
    df1.to_latex('variation_analysis.tex')

    ##
    df = pd.read_csv(fpn.main)

    ##
    df1 = df[[c.jd , c.jyr]]

    ##
    df1 = df1.drop_duplicates()
    dfo = df1.groupby(c.jyr).count()

    ##
    df1 = df[[c.ftic , c.jyr]].drop_duplicates()
    df2 = df1.groupby(c.jyr).count()

    ##
    dfo = dfo.merge(df2 , on = [c.jyr] , how = 'left')

    ##
    df1 = df[[cd.bsv , cd.ssv , cd.bdv , cd.sdv , c.jyr]]
    df2 = df1.groupby(c.jyr).sum() / 10 ** 9

    ##
    dfo = dfo.merge(df2 , on = [c.jyr] , how = 'left')

    ##
    df1 = df[[cd.bsva , cd.ssva , cd.bdva , cd.sdva , c.jyr]]
    df2 = df1.groupby(c.jyr).sum() / 10 ** 13

    ##
    dfo = dfo.merge(df2 , on = [c.jyr] , how = 'left')

    ##
    df1 = df[[cd.bsc , cd.ssc , cd.bdc , cd.sdc , c.jyr]]
    df2 = df1.groupby(c.jyr).mean()
    df2 = df2.rename(columns = lambda x : x + '_mean')
    df3 = df1.mean().to_frame().T.drop(columns = [c.jyr])
    df3 = df3.rename(columns = lambda x : x + '_mean')
    df2 = pd.concat([df2 , df3] , axis = 0)
    df2 = df2.reset_index()
    df2 = df2.rename(columns = {
            'index' : c.jyr
            })
    df2 = df2.set_index(c.jyr)

    ##
    df3 = df1.groupby(c.jyr).std()
    df3 = df3.rename(columns = lambda x : x + '_std')
    df4 = df1.std().to_frame().T.drop(columns = [c.jyr])
    df4 = df4.rename(columns = lambda x : x + '_std')
    df3 = pd.concat([df3 , df4] , axis = 0)
    df3 = df3.reset_index()
    df3 = df3.rename(columns = {
            'index' : c.jyr
            })
    df3 = df3.set_index(c.jyr)

    ##
    dfo = dfo.merge(df2 , on = [c.jyr] , how = 'outer')
    dfo = dfo.merge(df3 , on = [c.jyr] , how = 'outer')

    ##
    dfo.to_latex('sum_stat.tex')

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
