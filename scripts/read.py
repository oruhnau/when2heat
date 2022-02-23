
import os
import pandas as pd
import geopandas as gpd
import datetime as dt
from netCDF4 import Dataset, num2date
from shapely.geometry import Point


def temperature(input_path, year_start, year_end, parameter):


    df_list = []
    for year in range(year_start, year_end + 1):

        if parameter == "t2m":
            df_int = pd.concat(
                [weather(input_path, 'ERA_temperature_{}_{}.nc'.format('2m_temperature', year), parameter)],
                axis=0
            )

        if parameter == "stl1":
            df_int = pd.concat(
                [weather(input_path, 'ERA_temperature_{}_{}.nc'.format('soil_temperature_level_1', year), parameter)],
                axis=0
            )
        df_list.append(df_int)
    df = pd.concat(df_list, axis = 0)

    return df


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

    if variable_name == "si10":
        variable = variable[:, 0].data

    # Transform to pd.DataFrame
    index = pd.Index(num2date(time, time_units, only_use_python_datetimes=True), name='time')

    index = index.map(lambda x: dt.datetime(x.year, x.month, x.day, x.hour, x.minute, x.second))

    df = pd.DataFrame(data=variable.reshape(len(time), len(latitude) * len(longitude)),
                      index=index,
                      columns=pd.MultiIndex.from_product([latitude, longitude], names=('latitude', 'longitude')))

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


def heating_thresholds(input_path):

    file = os.path.join(input_path, 'heating_thresholds', 'heating_thresholds.csv')
    return pd.read_csv(file, sep=';', decimal=',', index_col=0)['Heating threshold']


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
                             'JRC_IDEES',
                             '{}_{}.csv'.format(building_type, heat_type)),
                decimal=',', index_col=0
            ).apply(pd.to_numeric, downcast='float')
            for building_type in ['Residential', 'Tertiary']
        }
        for heat_type in ['space', 'water']
    }

def cop_parameters(input_path):

    file = os.path.join(input_path, 'cop', 'cop_parameters.csv')
    return pd.read_csv(file, sep=';', decimal=',', header=0, index_col=0).apply(pd.to_numeric, downcast='float')
