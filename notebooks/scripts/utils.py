import re
import os
import glob
import requests
import pandas as pd
import numpy as np
import dask.dataframe as dd


def combine_australia_rainfall(base_folder_path="../data/raw/", method="pandas", delay_dask_compute=True):
    """
    Combines raw csv files for Australia rainfall from a download folder into one csv. 
    Adds `file_name` column and a `model` column with characters before first underscore in filename as the model name.

    Parameters
    ----------
    base_folder_path : str
        Path from which file names matching a pattern will be combined 
    method : str
        Specify to use either Pandas (method="pandas") or Dask (method="dask") to combine the csv files.
    delay_dask_compute: bool
        Should the Dask dataframe be computed to a Pandas DataFrame to return?


    Returns
    -------
    pandas.core.DataFrame or dask.DataFrame :
        DataFrame of all rainfall data. Pandas DataFrame by default, if method="dask" and delay_dask_compute=True will return a Dask DataFrame.

    """
    files = glob.glob(os.path.join(base_folder_path, "*.csv"))

    if method == "pandas":
        # TODO: Get the regex working here in one step to extract model name
        df = pd.concat((pd.read_csv(file, index_col=0)
                        .assign(file_name=re.findall("[ \w-]+\.", file)[0],
                                model=lambda x: x.file_name.str.split("_", expand=True)[0])
                        .drop(columns="file_name")
                        for file in files)
                       )
    elif method == "dask":
        df = dd.concat([dd.read_csv(file)
                        .assign(file_name=re.findall("[ \w-]+\.", file)[0])
                        for file in files]
                       )
        df['model'] = df['file_name'].str.partition('_')[0]
        if delay_dask_compute == False:
            df = df.compute()

    return df


if __name__ == "__main__":
    df = combine_australia_rainfall(method="dask")

    print(df["model"].value_counts().compute())
