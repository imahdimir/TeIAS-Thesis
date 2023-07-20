"""

    """

from pathlib import Path

from githubdata import GitHubDataRepo
from mirutil.df import save_df_as_prq

from main import c
from main import cn
from main import fpn
from main import gdu

def get_adj_price_data() :
    gdr = GitHubDataRepo(gdu.adj_price_s)
    df = gdr.read_data()
    return df

def keep_relevant_cols(df) :
    cols = {
            c.ftic   : None ,
            c.jd     : None ,
            c.aclose : None ,
            }

    return df[list(cols.keys())]

def make_month_col(df) :
    df[c.jm] = df[c.jd].str[:7]
    return df

def mark_month_highest_and_lowest(df) :
    dtyp = float

    gpo = df.groupby([c.ftic , c.jm])

    # find month highest value
    df[cn.acmx] = gpo[c.aclose].transform(lambda x : x.astype(dtyp).max())

    # mark month highest
    df[cn.mh] = df[c.aclose].astype(dtyp).eq(df[cn.acmx])

    # remove month lowest value
    df[cn.acmn] = gpo[c.aclose].transform(lambda x : x.astype(dtyp).min())

    # mark month lowest
    df[cn.ml] = df[c.aclose].astype(dtyp).eq(df[cn.acmn])

    return df

def main() :
    pass

    ##
    df = get_adj_price_data()

    ##
    df = keep_relevant_cols(df)

    ##
    df = make_month_col(df)

    ##
    df = mark_month_highest_and_lowest(df)

    ##
    save_df_as_prq(df , fpn.t11)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##
