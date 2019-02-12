
import numpy as np
import pandas as pd

from scripts.misc import localize, upsample_df


def reference_temperature(temperature):

    # Daily average
    daily_average = temperature.groupby(pd.Grouper(freq='D')).mean().copy()

    # Weighted mean
    weighted_mean = pd.DataFrame(columns=daily_average.columns)
    start = daily_average.index[0]
    for i in daily_average.index:
        weighted_mean.loc[i, :] = (daily_average.loc[i, :] +
                                   .5 * daily_average.loc[max(i-1, start), :] +
                                   .25 * daily_average.loc[max(i-2, start), :] +
                                   .125 * daily_average.loc[max(i-3, start), :]) / (1 + .5 + .25 + .125)

    return weighted_mean


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
    building_database = {
        'SFH': .7 * building_database['residential'],
        'MFH': .3 * building_database['residential'],
        'COM': building_database['commercial']
    }

    c_results = []
    for country, population in mapped_population.items():

        # Localize Timestamps (including daylight saving time correction)
        df_c = localize(df[country], country)

        cb_results = 0
        for building_type, building_data in building_database.items():

            # Weighting
            df_cb = df_c[building_type] * population

            # Scaling factors for the final energy demand in MW
            years = df_cb.index.year.unique()
            factors = pd.Series([
                building_data.loc[country, str(year)] / df_cb.loc[df_cb.index.year == year, ].sum().sum() * 1000000
                for year in years
            ], index=years)
            df_cb = df_cb.multiply(pd.Series(factors.loc[df_cb.index.year].values, index=df_cb.index), axis=0)

            # Transforming to heat demand assuming an average conversion efficiency of 0.9
            df_cb = .9 * df_cb

            # Cumulating building types
            cb_results += df_cb

        # Change index to UCT
        cb_results = cb_results.tz_convert('utc')
        c_results.append(cb_results)

    return pd.concat(c_results, keys=mapped_population.keys(), axis=1,
                     names=['country', 'latitude', 'longitude'])


def combine(space, water):

    # Aggregate spatially and round to zero decimals
    space = space.sum(level=0, axis=1, skipna=False).round()
    water = water.sum(level=0, axis=1, skipna=False).round()

    # Sum up and aggregate into one df
    heat = space + water
    df = pd.concat([heat, space, water],
                   axis=1, keys=['total', 'space', 'water'])

    # Swap Multiindex: County first, data second, unit third
    df = pd.concat([df], axis=1, keys=['heat_demand'])
    df = pd.concat([df], axis=1, keys=['MW'])
    df = df.swaplevel(i=0, j=3, axis=1)
    df = df.sort_index(level=0, axis=1)
    df.columns.names = ['country', 'variable', 'attribute', 'unit']

    return df
