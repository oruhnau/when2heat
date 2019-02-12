
import pandas as pd

from scripts.misc import localize


def source_temperature(temperature):

    celsius = temperature - 273.15

    return pd.concat(
        [celsius['air'], celsius['soil'] - 5, 0 * celsius['air'] + 10 - 3],
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
    )


def finishing(cop, demand_space, demand_water, correction_parameters):

    # Localize Timestamps (including daylight saving time correction) and convert to UTC
    countries = cop.columns.get_level_values('country').unique()
    cop = pd.concat(
        [localize(cop.swaplevel(0, 2, axis=1)[country], country).tz_convert('utc') for country in countries],
        keys=countries, axis=1
    ).swaplevel(0, 2, axis=1)

    # Spatial aggregation
    sources = cop.columns.get_level_values('source').unique()
    sinks = ['floor', 'radiator']
    heat = (demand_space + demand_water).sum(level=0, axis=1)
    power = pd.concat(
        [pd.concat(
            [(demand_space / cop[source][sink] + demand_water / cop[source]['water']).sum(level=0, axis=1)
             for sink in sinks],
            keys=sinks, axis=1
        ) for source in sources],
        keys=sources, axis=1, names=['source', 'sink', 'country']
    )
    cop = heat / power

    # Part load correction
    capacity_ratio = power / power.max()
    for source in sources:
        cr = capacity_ratio[source].clip(correction_parameters.loc[source, 'cr_min'])
        cdh = correction_parameters.loc[source, 'cdh']
        pump = correction_parameters.loc[source, 'pumping']
        cop[source] = cop[source] * pump * cr / (cdh * cr + 1 - cdh)

    # Round
    cop = cop.round(2)

    # Rename columns
    cop.columns.set_levels(['ASHP', 'GSHP', 'WSHP'], level=0, inplace=True)
    cop.columns.set_levels(['floor', 'radiator'], level=1, inplace=True)
    cop.columns = pd.MultiIndex.from_tuples([('_'.join([level for level in col_name[0:2]]), col_name[2]) for col_name in cop.columns.values])
    cop = pd.concat([cop], keys=['COP'], axis=1)
    cop = pd.concat([cop], keys=['coefficient'], axis=1)
    cop = cop.swaplevel(i=0, j=3, axis=1)
    cop = cop.sort_index(level=0, axis=1)
    cop.columns.names = ['country', 'variable', 'attribute', 'unit']

    return cop
