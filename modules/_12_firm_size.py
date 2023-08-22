"""

gets the firm size data and calcluates the firm size terciles

    """

from pathlib import Path

import pandas as pd
from githubdata.utils import get_data_fr_github_without_double_clone
from mirutil.df import save_df_as_prq

from main import c
from main import cn
from main import fpn
from main import gdu

def main() :
    pass

    ##
    df = get_data_fr_github_without_double_clone(gdu.mktcap_s)

    ##
    df[c.mktcap] = df[c.mktcap].astype('Int64')

    ##
    df[cn.fs_tercile] = pd.qcut(df[c.mktcap] , 3 , labels = False)

    ##
    save_df_as_prq(df , fpn.t12)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


def test() :
    pass

    ##

    ##
