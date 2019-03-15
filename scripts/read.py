
import os
import pandas as pd
import geopandas as gpd
from netCDF4 import Dataset, num2date
from shapely.geometry import Point


def temperature(input_path, year_start, year_end, param):

    return pd.concat(
        [weather(input_path, 'ERA_temperature_{}.nc'.format(year), param) for year in range(year_start, year_end+1)],
        axis=0
    )


def wind(input_path):

    return weather(input_path, 'ERA_wind.nc', 'si10')


def weather(input_path, filename, variable_name):

    file = os.path.join(input_path, 'weather', filename)

    # Read the netCDF file
    nc = Dataset(file)
    time = nc.variables['time'][:]
    time_units = nc.variables['time'].units
    latitude = nc.variables['latitude'][:]
    longitude = nc.variables['longitude'][:]
    variable = nc.variables[variable_name][:]

    # Transform to pd.DataFrame
    df = pd.DataFrame(data=variable.reshape(len(time), len(latitude) * len(longitude)),
                      index=pd.Index(num2date(time, time_units), name='time'),
                      columns=pd.MultiIndex.from_product([latitude, longitude],
                                                         names=('latitude', 'longitude')))

    return df


def population(input_path):

    directory = 'population/Version 2_0_1/'
    filename = 'GEOSTAT_grid_POP_1K_2011_V2_0_1.csv'

    # Read population data
    df = pd.read_csv(os.path.join(input_path, directory, filename),
                     usecols=['GRD_ID', 'TOT_P', 'CNTR_CODE'],
                     index_col='GRD_ID')

    # Make GeoDataFrame from the the coordinates in the index
    gdf = gpd.GeoDataFrame(df)
    gdf['geometry'] = df.index.map(lambda i: Point(
        [1000 * float(x) + 500 for x in reversed(i.split('N')[1].split('E'))]
    ))

    # Transform coordinate reference system to 'latitude/longitude'
    gdf.crs = {'init': 'epsg:3035'}

    return gdf


def daily_parameters(input_path):

    file = os.path.join(input_path, 'bgw_bdew', 'daily_demand.csv')
    return pd.read_csv(file, sep=';', decimal=',', header=[0, 1], index_col=0)


def hourly_parameters(input_path):

    def read():
        file = os.path.join(input_path, 'bgw_bdew', filename)
        return pd.read_csv(file, sep=';', decimal=',', index_col=index_col).apply(pd.to_numeric, downcast='float')

    parameters = {}
    for building_type in ['SFH', 'MFH', 'COM']:

        filename = 'hourly_factors_{}.csv'.format(building_type)

        # MultiIndex for commercial heat because of weekday dependency
        index_col = [0, 1] if building_type == 'COM' else 0

        parameters[building_type] = read()

    return parameters


def building_database(input_path):

    return {
        heat_type: {
            building_type: pd.read_csv(
                os.path.join(input_path,
                             'eu_building_database',
                             '{}_{}.csv'.format(building_type, heat_type)),
                sep=';', decimal=',', index_col=0
            ).apply(pd.to_numeric, downcast='float')
            for building_type in ['residential', 'commercial']
        }
        for heat_type in ['space', 'water']
    }


def cop_parameters(input_path):

    file = os.path.join(input_path, 'cop', 'cop_parameters.csv')
    return pd.read_csv(file, sep=';', decimal=',', header=0, index_col=0).apply(pd.to_numeric, downcast='float')
