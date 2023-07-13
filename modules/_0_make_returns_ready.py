"""

    """

from pathlib import Path

import numpy as np
from githubdata import GitHubDataRepo
from scipy import stats

from main import c
from main import cn
from main import fpn
from main import gdu
from main import pa

def keep_relevant_cols(df) :
    # remove not wanted cols
    return df.drop(columns = [c.arlo , c.ar1d])

def remove_n_days_after_ipo(df) :
    """remove some days after ipo which specified by a parameter"""

    # first sort on date
    df = df.sort_values(by = [c.d])

    # groupby by firm
    g = df.groupby(c.ftic)

    # remove some days after the ipo for each firm

    n = pa.days_2_rm_after_ipo
    _df = g.nth[n :]

    return _df

def main() :
    pass

    ##
    # dl filled returns data
    gdr = GitHubDataRepo(gdu.src_adj_rets)
    
    ##
    df = gdr.read_data()

    ##
    df = keep_relevant_cols(df)

    ##
    df = remove_n_days_after_ipo(df)

    ##
    # copy the returns column for making not valid values to nan
    df[cn.ret] = df[c.ar1dlf].copy()

    ##
    # remove inf vals
    df[cn.ret] = df[cn.ret].replace([np.inf , -np.inf] , np.nan)

    ##
    # find Z score for outlier filtering
    df['z'] = np.abs(stats.zscore(df[cn.ret].dropna().astype(float)))

    ##
    # remove outliers
    msk = df['z'].gt(2)
    df.loc[msk , cn.ret] = np.nan

    ##
    df = df.drop(columns = ['z'])

    ##
    df.to_parquet(fpn.t0 , index = False)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


# noinspection PyUnreachableCode
if False :
    pass

    ##
