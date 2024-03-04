
import os
import sqlite3
import pandas as pd


def shaping(demand, cop):
    print("index:")

    # Merge demand and cop
    df = pd.concat([demand, cop], axis=1)

    df = df.sort_index(level=0, axis=1)

    # Timestamp
    index = pd.DatetimeIndex(df.index)
    df.index = pd.MultiIndex.from_tuples(zip(
        index.strftime('%Y-%m-%dT%H:%M:%SZ'),
        index.tz_convert('Europe/Brussels').strftime('%Y-%m-%dT%H:%M:%S%z')
    ))
    df.index.names = ['utc_timestamp', 'cet_cest_timestamp']

    # SingleIndex
    single = df.copy()
    single.columns = ['_'.join([level for level in col_name[0:3]]) for col_name in df.columns.values]
    single.insert(0, 'cet_cest_timestamp', single.index.get_level_values(1))
    single.index = single.index.droplevel(['cet_cest_timestamp'])

    # Stacked
    stacked = df.copy()
    stacked.index = stacked.index.droplevel(['cet_cest_timestamp'])
    stacked.columns = stacked.columns.droplevel(['unit'])
    stacked = stacked.transpose().stack(dropna=True).to_frame(name='data')

    # Excel
    df_excel = df.copy()
    df_excel.index = pd.MultiIndex.from_tuples(zip(
        index.strftime('%Y-%m-%dT%H:%M:%SZ'),
        index.tz_convert('Europe/Brussels').strftime('%Y-%m-%dT%H:%M:%S')
    ))

    return {
        'multiindex': df,
        'singleindex': single,
        'stacked': stacked,
        'excel': df_excel
    }


def to_sql(shaped_dfs, output_path, home_path):

    os.chdir(output_path)
    table = 'when2heat'
    shaped_dfs['singleindex'].to_sql(table, sqlite3.connect('when2heat.sqlite'),
                                     if_exists='replace', index_label='utc_timestamp')
    os.chdir(home_path)


def to_csv(shaped_dfs, output_path):

    for shape, df in shaped_dfs.items():
        if shape == 'excel':
            # file = os.path.join(output_path, 'when2heat.xlsx.csv')
            file = os.path.join(output_path, 'when2heat.xlsx')
            df.to_csv(file, sep=';', decimal=',', float_format='%g')

        elif shape == 'singleindex':
            file = os.path.join(output_path, 'when2heat.csv')
            df.to_csv(file, sep=';', decimal=',', float_format='%g')

        else:
            file = os.path.join(output_path, 'when2heat_{}.csv'.format(shape))
            df.to_csv(file, float_format='%g')
