
import pytz
import pandas as pd


def localize(df, country, ambiguous=None):

    # The exceptions below correct for daylight saving time
    try:
        df.index = df.index.tz_localize(pytz.country_timezones[country][0], ambiguous=ambiguous)
        return df

    # Delete values that do not exist because of daylight saving time
    except pytz.NonExistentTimeError as err:
        return localize(df.loc[df.index != err.args[0], ], country)

    # Duplicate values that exist twice because of daylight saving time
    except pytz.AmbiguousTimeError as err:
        idx = pd.Timestamp(err.args[0].split("from ")[1].split(",")[0])
        unambiguous_df = localize(df.loc[df.index != idx, ], country)
        ambiguous_df = localize(df.loc[[idx, idx], ], country, ambiguous=[True, False])

        # updated to new Pandas
        return pd.concat([unambiguous_df, ambiguous_df]).sort_index()
        # return unambiguous_df.append(ambiguous_df).sort_index()


def upsample_df(df, resolution):

    # The low-resolution values are applied to all high-resolution values up to the next low-resolution value
    # In particular, the last low-resolution value is extended up to where the next low-resolution value would be

    df = df.copy()

    # Determine the original frequency
    freq = df.index[-1] - df.index[-2]

    # Temporally append the DataFrame by one low-resolution value
    df.loc[df.index[-1] + freq, :] = df.iloc[-1, :]

    # Up-sample
    # Deprecated since version 2.0
    # df = df.resample(resolution).pad()
    df = df.resample(resolution).ffill()

    # Drop the temporal low-resolution value
    df.drop(df.index[-1], inplace=True)

    return df


def group_df_by_multiple_column_levels(df, column_levels):

    df = df.groupby(df.columns.droplevel(list(set(df.columns.names) - set(column_levels))), axis=1).sum()
    df.columns = pd.MultiIndex.from_tuples(df.columns, names=column_levels)

    return df
