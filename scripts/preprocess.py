
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import scripts.read as read
from scripts.misc import upsample_df


def map_population(input_path, countries, interim_path, plot=True):

    population = None
    weather_grid = None
    mapped_population = {}

    for country in countries:

        file = os.path.join(interim_path, 'population_{}'.format(country))

        if not os.path.isfile(file):

            if population is None:

                population = read.population(input_path)
                weather_data = read.wind(input_path)  # For the weather grid

                # Make GeoDataFrame from the weather data coordinates
                weather_grid = gpd.GeoDataFrame(index=weather_data.columns)
                weather_grid['geometry'] = weather_grid.index.map(lambda i: Point(reversed(i)))

                # Set coordinate reference system to 'latitude/longitude'
                weather_grid.crs = {'init': 'epsg:4326'}

                # Make polygons around the weather points
                weather_grid['geometry'] = weather_grid.geometry.apply(lambda point: point.buffer(.75 / 2, cap_style=3))

                # Make list from MultiIndex (this is necessary for the spatial join)
                weather_grid.index = weather_grid.index.tolist()

            # For Luxembourg, a single weather grid point is manually added for lack of population geodata
            if country == 'LU':
                s = pd.Series({(49.5, 6): 1})

            else:

                # Filter population data by country to cut processing time
                if country == 'GB':
                    gdf = population[population['CNTR_CODE'] == 'UK'].copy()
                else:
                    gdf = population[population['CNTR_CODE'] == country].copy()

                # Align coordinate reference systems
                gdf = gdf.to_crs({'init': 'epsg:4326'})

                # Spatial join
                gdf = gpd.sjoin(gdf, weather_grid, how="left", op='within')

                # Sum up population
                s = gdf.groupby('index_right')['TOT_P'].sum()

            # Write results to interim path
            s.to_pickle(file)

        else:

            s = pd.read_pickle(file)
            print('{} already exists and is read from disk.'.format(file))

        mapped_population[country] = s

    if plot:
        print('Plot of the re-mapped population data of {} (first selected country) '
              'for visual inspection:'.format(countries[0]))
        gdf = gpd.GeoDataFrame(mapped_population[countries[0]], columns=['TOT_P'])
        gdf['geometry'] = gdf.index.map(lambda i: Point(reversed(i)))
        gdf.plot(column='TOT_P')

    return mapped_population


def wind(input_path, mapped_population, plot=True):

    df = read.wind(input_path)

    # Temporal average
    s = df.mean(0)

    if plot:
        print('Plot of the wind averages for visual inspection:')
        gdf = gpd.GeoDataFrame(s, columns=['wind'])
        gdf['geometry'] = gdf.index.map(lambda i: Point(reversed(i)))
        gdf.plot(column='wind')

    # Wind data is filtered by country
    return pd.concat(
        [s[population.index] for population in mapped_population.values()],
        keys=mapped_population.keys(), names=['country', 'latitude', 'longitude'], axis=0
    ).apply(pd.to_numeric, downcast='float')


def temperature(input_path, year_start, year_end, mapped_population):

    parameters = {
        'air': 't2m',
        'soil': 'stl4'
    }

    t = pd.concat(
        [read.temperature(input_path, year_start, year_end, parameter) for parameter in parameters.values()],
        keys=parameters.keys(), names=['parameter', 'latitude', 'longitude'], axis=1
    )

    t = upsample_df(t, '60min')

    # Temperature data is filtered by country
    return pd.concat(
        [pd.concat(
            [t[parameter][population.index] for population in mapped_population.values()],
            keys=mapped_population.keys(), axis=1
        ) for parameter in parameters.keys()],
        keys=parameters.keys(), names=['parameter', 'country', 'latitude', 'longitude'], axis=1
    ).apply(pd.to_numeric, downcast='float')
