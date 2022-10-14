

def modify_date(df, step, nyears):
    """modify_date(df)

    Modify DatetimeIndex column for reforecast data.

    Params
    ------
    df : pandas.DataFrame
        Data.frame with a DatetimeIndex column to be modified
    step : int
        Froe cast step, lead time in hours.
    nyears : int or None
        Number of years. Must be known as '1' in the multiindex
        is 'nyears ago'. None is used if forecasts (not reforecasts)
        are processed where the current date is the correct one.

    Return
    ------
    pandas.DataFrame : Returns a manipulated version of the input 'df'.
    """
    import numpy as np
    import pandas as pd
    import datetime as dt

    assert isinstance(df, pd.DataFrame), TypeError("argument 'df' must be a pandas.DataFrame")
    assert isinstance(step, np.timedelta64), TypeError("argument 'step' must be numpy.timedelta64")
    assert isinstance(nyears, (int, type(None))), TypeError("argument 'nyears' must be int or None")

    if isinstance(df.index, pd.MultiIndex) and nyears is not None:
        result = []
        for rec in df.index:
            x = rec[0].utctimetuple()        # Getting date from index
            years = int(nyears - rec[1] + 1) # Years offset
            tmp = np.datetime64(dt.datetime(x.tm_year - years, x.tm_mon, x.tm_mday, x.tm_hour, x.tm_min))
            # DEVEL
            if tmp in result: raise Exception("modify_date starts to create duplicated dates")
            result.append(tmp)
    else:
        result = df.index

    result = [x + step for x in result]
    df.index = pd.DatetimeIndex(result, name = "valid_time")
    return df
