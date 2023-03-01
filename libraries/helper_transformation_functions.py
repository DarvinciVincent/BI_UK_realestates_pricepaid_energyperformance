from datetime import datetime
import pandas as pd
import numpy as np

from typing import Tuple


def get_current_datetime() -> Tuple[datetime, int, int]:
    current_date = datetime.today().strftime("%Y%m%d")
    current_month = datetime.now().month
    current_year = datetime.now().year
    
    return current_date, current_month, current_year

# dataframe-related
def standardardize_nan(df, patterns, standard="") -> pd.DataFrame:
    df = df.replace(patterns, standard)
    df = df.fillna(np.nan)
    return df

def standardardize_column(df, column_name, patterns, isregex=True) -> pd.DataFrame:
    if isregex:
        df[f'{column_name}'] = df[f'{column_name}'].replace(regex=patterns)
    else:
        df[f'{column_name}'] = df[f'{column_name}'].replace(patterns)
    return df