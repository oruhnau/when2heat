
import numpy as np
import pandas as pd

from scripts.misc import localize, upsample_df, group_df_by_multiple_column_levels


def reference_temperature(temperature):

    # Daily average
    daily_average = temperature.groupby(pd.Grouper(freq='D')).mean().copy()

    # Weighted mean
    return sum([.5 ** i * daily_average.shift(i).fillna(method='bfill') for i in range(4)]) / \
           sum([.5 ** i for i in range(4)])


def daily_heat(temperature, wind, all_parameters):

    # BDEW et al. 2015 describes the function for the daily heat demand
    # This is implemented in the following and passed to the general daily function

    def heat_function(t, parameters):

        celsius = t - 273.15  # The temperature input is in Kelvin

        sigmoid = parameters['A'] / (
                1 + (parameters['B'] / (celsius - 40)) ** parameters['C']
        ) + parameters['D']

        linear = pd.DataFrame(
            [parameters['m_{}'.format(i)] * celsius + parameters['b_{}'.format(i)] for i in ['s', 'w']]
        ).max()

        return sigmoid + linear

    return daily(temperature, wind, all_parameters, heat_function)


def daily_water(temperature, wind, all_parameters):

    # A function for the daily water heating demand is derived from BDEW et al. 2015
    # This is implemented in the following and passed to the general daily function

    def water_function(t, parameters):

        celsius = t - 273.15  # The temperature input is in Kelvin

        # Below 15 °C, the water heating demand is not defined and assumed to stay constant
        celsius.clip(15, inplace=True)

        return parameters['m_w'] * celsius + parameters['b_w'] + parameters['D']

    return daily(temperature, wind, all_parameters, water_function)


def daily(temperature, wind, all_parameters, func):

    # All locations are separated by the average wind speed with the threshold 4.4 m/s
    windy_locations = {
        'normal': wind[wind > 4.4].index,
        'windy': wind[wind <= 4.4].index
    }

    buildings = ['SFH', 'MFH', 'COM']

    return pd.concat(
        [pd.concat(
            [temperature[locations].apply(func, parameters=all_parameters[(building, windiness)])
             for windiness, locations in windy_locations.items()],
            axis=1
        ) for building in buildings],
        keys=buildings, names=['building', 'country', 'latitude', 'longitude'], axis=1
    )


def hourly_heat(daily_df, temperature, parameters):

    # According to BGW 2006, temperature classes are derived from the temperature data
    # This is re-sampled to a 60-min-resolution and passed to the general hourly function

    classes = upsample_df(
        (np.ceil(((temperature - 273.15) / 5).astype('float64')) * 5).clip(lower=-15, upper=30),
        '60min'
    ).astype(int).astype(str)

    return hourly(daily_df, classes, parameters)


def hourly_water(daily_df, temperature, parameters):

    # For water heating, the highest temperature classes '30' is chosen
    # This is re-sampled to a 60-min-resolution and passed to the general hourly function

    classes = upsample_df(
        pd.DataFrame(30, index=temperature.index, columns=temperature.columns),
        '60min'
    ).astype(int).astype(str)

    return hourly(daily_df, classes, parameters)


def hourly(daily_df, classes, parameters):

    def hourly_factors(building):

        # This function selects hourly factors from BGW 2006 by time and temperature class
        slp = pd.DataFrame(index=classes.index, columns=classes.columns)

        # Time includes the hour of the day
        times = classes.index.map(lambda x: x.strftime('%H:%M'))
        # For commercial buildings, time additionally includes the weekday
        if building == 'COM':
            weekdays = classes.index.map(lambda x: int(x.strftime('%w')))
            times = list(zip(weekdays, times))

        for column in classes.columns:
            slp[column] = parameters[building].lookup(times, classes.loc[:, column])

        return slp

    buildings = daily_df.columns.get_level_values('building').unique()

    results = pd.concat(
        [upsample_df(daily_df, '60min')[building] * hourly_factors(building) for building in buildings],
        keys=buildings, names=['building', 'country', 'latitude', 'longitude'], axis=1
    )

    return results.swaplevel('building', 'country', axis=1)


def finishing(df, mapped_population, building_database):

    # Single- and multi-family houses are aggregated assuming a ratio of 70:30
    # Transforming to heat demand assuming an average conversion efficiency of 0.9
    building_database = {
        'SFH': .9 * .7 * building_database['residential'],
        'MFH': .9 * .3 * building_database['residential'],
        'COM': .9 * building_database['commercial']
    }

    results = []
    for country, population in mapped_population.items():

        # Localize Timestamps (including daylight saving time correction)
        df_country = localize(df[country], country)

        normalized = []
        absolute = []
        for building_type, building_data in building_database.items():

            # Weighting
            df_cb = df_country[building_type] * population

            # Scaling to 1 TWh/a
            years = df_cb.index.year.unique()
            factors = pd.Series([
                1000000 / df_cb.loc[df_cb.index.year == year, ].sum().sum()
                for year in years
            ], index=years)
            normalized.append(df_cb.multiply(
                pd.Series(factors.loc[df_cb.index.year].values, index=df_cb.index), axis=0
            ))

            # Scaling to building database
            database_years = building_data.columns
            factors = pd.Series([
                building_data.loc[country, str(year)] if str(year) in database_years else float('nan')
                for year in years
            ], index=years)
            absolute.append(normalized[-1].multiply(
                pd.Series(factors.loc[df_cb.index.year].values, index=df_cb.index), axis=0, fill_value=None
            ))

        country_results = pd.concat(
            [pd.concat(x, axis=1, keys=building_database.keys()) for x in [normalized, absolute]],
            axis=1, keys=['MW/TWh', 'MW']
        ).apply(pd.to_numeric, downcast='float')

        # Change index to UCT
        results.append(country_results.tz_convert('utc'))

    return pd.concat(results, keys=mapped_population.keys(), axis=1,
                     names=['country', 'unit', 'building_type', 'latitude', 'longitude'])


def combine(space, water):

    # Spatial aggregation
    space = group_df_by_multiple_column_levels(space, ['country', 'unit', 'building_type'])
    water = group_df_by_multiple_column_levels(water, ['country', 'unit', 'building_type'])

    # Merge space and water
    df = pd.concat([space, water], axis=1, keys=['space', 'water'],
                   names=['attribute', 'country', 'unit', 'building_type'])

    # Aggregation of building types for absolute values
    dfx = df.loc[:, df.columns.get_level_values('unit') == 'MW']
    dfx = dfx.groupby(dfx.columns.droplevel('building_type'), axis=1).sum()
    dfx.columns = pd.MultiIndex.from_tuples(dfx.columns)
    dfx = pd.concat([dfx['space'], dfx['water'], dfx['space'] + dfx['water']], axis=1,
                    keys=['space', 'water', 'total'], names=['attribute', 'country', 'unit'])

    # Rename columns
    df.columns = pd.MultiIndex.from_tuples(
        [('_'.join([level for level in [col_name[0], col_name[3]]]), col_name[1], col_name[2])
         for col_name in df.columns.values]
    )

    # Combine building-specific and aggregated time series, round, restore nan
    df = pd.concat([dfx, df], axis=1).round()
    df.replace(0, float('nan'), inplace=True)

    # Fill NA at the end and the beginning of the dataset arising from different local times
    df = df.fillna(method='bfill').fillna(method='ffill')

    # Swap MultiIndex
    df = pd.concat([
        df.loc[:, df.columns.get_level_values('unit') == 'MW'],
        df.loc[:, df.columns.get_level_values('unit') == 'MW/TWh']
    ], axis=1, keys=['heat_demand', 'heat_profile'])
    df = df.swaplevel(i=0, j=2, axis=1)
    df = df.swaplevel(i=1, j=2, axis=1)
    df = df.sort_index(level=0, axis=1)
    df.columns.names = ['country', 'variable', 'attribute', 'unit']

    return df
