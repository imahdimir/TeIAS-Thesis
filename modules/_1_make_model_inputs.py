"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo

from main import c
from main import cn
from main import fpn
from main import gdu

def get_risk_free_rate_data() :
    gdr = GitHubDataRepo(gdu.src_rf)
    gdr.clone_overwrite()
    df = gdr.read_data()
    gdr.rmdir()
    return df

def add_risk_free_rate(df , df_rf) :
    df[c.jm] = df[c.jd].str[:7]

    df_rf = df_rf[[c.jm , c.rf_d]]

    return df.merge(df_rf , how = 'left')

def get_market_index_data() :
    gdr = GitHubDataRepo(gdu.mkt_indx_s)
    df = gdr.read_data()
    return df

def cal_market_return(df_mkt) :
    df = df_mkt[[c.jd , c.tedpix_close]]
    df = df.sort_values(c.jd)
    df[c.tedpix_ret] = df[c.tedpix_close].astype(float).pct_change()
    return df

def add_market_return(df , df_mkt) :
    df_mkt = df_mkt[[c.jd , c.tedpix_ret]]
    df = df.merge(df_mkt , how = 'left')
    return df

def cal_excess_returns(df) :
    df[c.tedpix_exss_ret] = df[c.tedpix_ret] - df[c.rf_d]
    df[c.exss_ret] = df[cn.ret] - df[c.rf_d]
    return df

def main() :
    pass

    ##
    # read returns data
    df = pd.read_parquet(fpn.t0)

    ##
    df_rf = get_risk_free_rate_data()

    ##
    df = add_risk_free_rate(df , df_rf)

    ##
    df_mkt = get_market_index_data()

    ##
    df_mkt = cal_market_return(df_mkt)

    ##
    df = add_market_return(df , df_mkt)

    ##
    df = cal_excess_returns(df)

    ##
    df.to_parquet(fpn.t1 , index = False)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
