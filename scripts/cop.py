
import os
import pandas as pd

from scripts.misc import localize
from scripts.misc import group_df_by_multiple_column_levels


def source_temperature(temperature):

    celsius = temperature - 273.15

    return pd.concat(
        [celsius['air'], celsius['soil'] - 5, 0 * celsius['air'] + 10 - 5],
        keys=['air', 'ground', 'water'],
        names=['source', 'country', 'latitude', 'longitude'],
        axis=1
    )


def sink_temperature(temperature):

    celsius = temperature['air'] - 273.15

    return pd.concat(
        [-1 * celsius + 40, -.5 * celsius + 30, 0 * celsius + 50],
        keys=['radiator', 'floor', 'water'],
        names=['sink', 'country', 'latitude', 'longitude'],
        axis=1
    )


def spatial_cop(source, sink, cop_parameters):

    def cop_curve(delta_t, source_type):
        delta_t.clip(lower=15, inplace=True)
        return sum(cop_parameters.loc[i, source_type] * delta_t ** i for i in range(3))

    source_types = source.columns.get_level_values('source').unique()
    sink_types = sink.columns.get_level_values('sink').unique()

    return pd.concat(
        [pd.concat(
            [cop_curve(sink[sink_type] - source[source_type], source_type)
             for sink_type in sink_types],
            keys=sink_types,
            axis=1
        ) for source_type in source_types],
        keys=source_types,
        axis=1,
        names=['source', 'sink', 'country', 'latitude', 'longitude']
    ).round(4).swaplevel(0, 2, axis=1)


def finishing(cop, demand_space, demand_water, correction=.85):

    # Localize Timestamps (including daylight saving time correction) and convert to UTC
    countries = cop.columns.get_level_values('country').unique()
    sinks = cop.columns.get_level_values('sink').unique()
    cop = pd.concat(
        [pd.concat(
            [pd.concat(
                [localize(cop[country][sink], country).tz_convert('utc') for sink in sinks],
                keys=sinks, axis=1
            )], keys=[country], axis=1
        ).swaplevel(0, 2, axis=1) for country in countries],
        axis=1, names=['source', 'sink', 'country', 'latitude', 'longitude']
    )

    # Prepare demand values
    demand_space = demand_space.loc[:, demand_space.columns.get_level_values('unit') == 'MW/TWh']
    demand_space = group_df_by_multiple_column_levels(demand_space, ['country', 'latitude', 'longitude'])

    demand_water = demand_water.loc[:, demand_water.columns.get_level_values('unit') == 'MW/TWh']
    demand_water = group_df_by_multiple_column_levels(demand_water, ['country', 'latitude', 'longitude'])

    # Spatial aggregation
    sources = cop.columns.get_level_values('source').unique()
    sinks = cop.columns.get_level_values('sink').unique()
    power = pd.concat(
        [pd.concat(
            [(demand_water / cop[source][sink]).sum(level=0, axis=1)
             if sink == 'water' else
             (demand_space / cop[source][sink]).sum(level=0, axis=1)
             for sink in sinks],
            keys=sinks, axis=1
        ) for source in sources],
        keys=sources, axis=1, names=['source', 'sink', 'country']
    )
    heat = pd.concat(
        [pd.concat(
            [demand_water.sum(level=0, axis=1)
             if sink == 'water' else
             demand_space.sum(level=0, axis=1)
             for sink in sinks],
            keys=sinks, axis=1
        ) for source in sources],
        keys=sources, axis=1, names=['source', 'sink', 'country']
    )
    cop = heat / power

    # Correction and round
    cop = (cop * correction).round(2)

    # Fill NA at the end and the beginning of the dataset arising from different local times
    cop = cop.fillna(method='bfill').fillna(method='ffill')

    # Rename columns
    cop.columns.set_levels(['ASHP', 'GSHP', 'WSHP'], level=0, inplace=True)
    cop.columns.set_levels(['radiator', 'floor', 'water'], level=1, inplace=True)
    cop.columns = pd.MultiIndex.from_tuples([('_'.join([level for level in col_name[0:2]]), col_name[2]) for col_name in cop.columns.values])
    cop = pd.concat([cop], keys=['COP'], axis=1)
    cop = pd.concat([cop], keys=['coefficient'], axis=1)
    cop = cop.swaplevel(i=0, j=3, axis=1)
    cop = cop.sort_index(level=0, axis=1)
    cop.columns.names = ['country', 'variable', 'attribute', 'unit']

    return cop


def validation(cop, heat, output_path, corrected):

    def averages(df):
        return pd.concat(
            [df.loc[(df.index >= pd.Timestamp(year=2011, month=7, day=1, tz='utc'))
                    & (df.index < pd.Timestamp(year=2012, month=7, day=1, tz='utc')), ].sum(),
             df.loc[(df.index >= pd.Timestamp(year=2012, month=7, day=1, tz='utc'))
                    & (df.index < pd.Timestamp(year=2013, month=7, day=1, tz='utc')), ].sum()],
            keys=['2011/2012', '2012/2013'], axis=1
        )

    # Data preparation
    cop = cop['DE']['COP']
    cop.columns = cop.columns.droplevel(1)

    heat = heat['DE']['heat_profile']
    heat.columns = heat.columns.droplevel(1)

    # Power calculation
    power = pd.DataFrame()
    for heat_pump in ['ASHP', 'GSHP', 'WSHP']:
        power[heat_pump] = (
            .8 * .85 * heat['space_SFH'] / cop['{}_{}'.format(heat_pump, 'floor')] +
            .8 * .15 * heat['space_SFH'] / cop['{}_{}'.format(heat_pump, 'radiator')] +
            .2 * heat['water_SFH'] / cop['{}_water'.format(heat_pump)]
        )
    heat = pd.concat([.8 * heat['space_SFH'] + .2 * heat['water_SFH']]*3, axis=1, keys=power.columns)

    # Monthly aggregation
    heat = averages(heat)
    power = averages(power)

    cop = heat/power

    cop.round(2).to_csv(os.path.join(output_path, 'cop_{}.csv'.format(corrected)), sep=';', decimal=',')
