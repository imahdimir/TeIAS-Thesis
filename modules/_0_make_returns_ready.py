"""

    """

import numpy as np
from githubdata import GitHubDataRepo
from scipy import stats

from main import c
from main import fpn
from main import gdu
from main import pa

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
    gdr = GitHubDataRepo(gdu.adj_ret)

    ##
    df = gdr.read_data()

    ##
    df = remove_n_days_after_ipo(df)

    ##
    # find Z score for outlier filtering
    df['z'] = np.abs(stats.zscore(df[c.ar1dlf]))

    ##
    # remove outliers
    msk = df['z'].le(3)
    df = df[msk]

    ##
    df = df.drop(columns = ['z'])

    ##
    df.to_parquet(fpn.t0 , index = False)

##
if __name__ == "__main__" :
    main()
