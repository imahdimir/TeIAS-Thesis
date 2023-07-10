"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo as GDR

from main import c
from main import cn
from main import fpn
from main import gdu
from main import ns

# namespace  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
cd = ns.DInsIndCols()

def main() :
    pass

    ##

    # read measures data
    df = pd.read_parquet(fpn.t8)

    ##

    ##

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


# noinspection PyUnreachableCode
if False :
    pass

    ##
