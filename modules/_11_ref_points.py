"""

    """

from pathlib import Path

from githubdata import GitHubDataRepo
from mirutil.df import save_df_as_prq

from main import c
from main import fpn
from main import gdu

def get_tse_working_days_data() :
    gdr = GitHubDataRepo(gdu.tse_wds_s)
    df = gdr.read_data()
    return df

def keep_relevant_cols(df) :
    # remove not wanted cols
    return df[[c.jd , c.wd]]

def main() :
    pass

    ##
    df = get_tse_working_days_data()

    ##
    df = keep_relevant_cols(df)

    ##
    save_df_as_prq(df , fpn.t10)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


# noinspection PyUnreachableCode
if False :
    pass

    ##
