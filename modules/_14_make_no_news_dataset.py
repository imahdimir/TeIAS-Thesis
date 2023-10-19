"""

    """

import pandas as pd

from main import *
from modules._13_make_main_dataset import gen_jyear

def mark_news_neighborhood_by_firm(df , days_to_remove) :
    has_news = 'Has_News'

    has_news_types = [nws_type.good , nws_type.neutral , nws_type.bad ,
                      nws_type.unk]

    df[has_news] = df[cn.nws_type].isin(has_news_types)

    msk = df[has_news].eq(False)

    df.loc[msk , has_news] = None

    nhbr1 = 'nhbr1'
    nhbr2 = 'nhbr2'

    df = df.sort_values(by = [c.d] , ascending = False)

    df[nhbr1] = df.groupby(c.ftic)[has_news].ffill(days_to_remove)
    df[nhbr2] = df.groupby(c.ftic)[has_news].bfill(days_to_remove)

    nhbr = 'News_Neighborhood'

    msk = df[nhbr1].eq(True)
    msk |= df[nhbr2].eq(True)

    df.loc[msk , nhbr] = True

    df = df.drop(columns = [has_news , nhbr1 , nhbr2])

    return df

def drop_non_eligible_rows(df) :
    msk = df[c.is_tic_open].eq(True)
    msk &= df[cn.nws_type].eq(nws_type.no_news)
    msk &= df[cn.nhbr].isna()

    df = df[msk]

    return df

def drop_excess_cols(df) :
    cols = {
            cn.nws_type : None ,
            cn.nhbr     : None ,
            }

    df = df.drop(columns = cols.keys())

    return df

def main() :
    pass

    ##

    df = pd.read_parquet(fpn.t13_0)

    ##
    df3 = mark_news_neighborhood_by_firm(df , 3)
    df5 = mark_news_neighborhood_by_firm(df , 5)

    ##
    df3 = drop_non_eligible_rows(df3)
    df5 = drop_non_eligible_rows(df5)

    ##
    df3 = drop_excess_cols(df3)
    df5 = drop_excess_cols(df5)

    ##
    df3 = gen_jyear(df3)
    df5 = gen_jyear(df5)

    ##
    df3.to_csv(fpn.no_nws3 , index = False)
    df5.to_csv(fpn.no_nws5 , index = False)

##
if __name__ == "__main__" :
    main()
