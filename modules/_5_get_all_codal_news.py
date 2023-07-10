"""

    """

from pathlib import Path

from githubdata import GitHubDataRepo
from namespace_mahdimir import tse as tse_ns

from main import c
from main import fpn
from main import gdu

# namespace  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
cd = tse_ns.DAllCodalLetters()

def get_all_codal_letters() :
    """get all codal letters data"""
    gdr = GitHubDataRepo(gdu.src_codal_ltrs)
    gdr.clone_overwrite()
    df = gdr.read_data()
    gdr.rmdir()
    return df

def keep_relevant_cols(df) :
    # remove not wanted cols
    cols_2_keep = {
            cd.CodalTicker     : None ,
            cd.PublishDateTime : None ,
            }

    return df[list(cols_2_keep.keys())]

def get_codal_letters_to_firms_tickers_map() :
    """get codal letters to firms tickers map"""
    gdr = GitHubDataRepo(gdu.src_codal_tics_2_ftics)
    gdr.clone_overwrite()
    df = gdr.read_data()
    gdr.rmdir()
    return df

def map_codal_letters_to_firms_tickers(df , df_tics) :
    """map codal letters to firms tickers, and drop codal letters ticker col , and nan firm tickers"""
    c1 = c.ftic
    c2 = cd.CodalTicker

    df[c1] = df[c2].map(df_tics.set_index(c2)[c.ftic])

    df = df.drop(columns = [c2])

    df = df.dropna()

    return df

def main() :
    pass

    ##
    df = get_all_codal_letters()

    ##
    df = keep_relevant_cols(df)

    ##
    # drop duplicates
    df = df.drop_duplicates()

    ##
    df_tics = get_codal_letters_to_firms_tickers_map()

    ##
    df = map_codal_letters_to_firms_tickers(df , df_tics)

    ##
    # save data
    df.to_parquet(fpn.t5 , index = False)

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
