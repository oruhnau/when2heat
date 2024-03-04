
import os
import ssl
import datetime
import urllib
import zipfile
import cdsapi
from IPython.display import clear_output



def wind(input_path):
    filename = 'ERA_wind.nc'
    weather_path = os.path.join(input_path, 'weather')
    os.makedirs(weather_path, exist_ok=True)
    file = os.path.join(weather_path, filename)

    if not os.path.isfile(file):

        # Select all months from 1979 to 2021 by the date of the first day of the month
        data_package = 'reanalysis-era5-single-levels-monthly-means'
        variable = "10m_wind_speed"
        product_type = 'monthly_averaged_reanalysis'
        dates = {
        'year': [str(year) for year in range(1979, 2022)],
        'month': ["%.2d" % month for month in range(1, 13)],
        'time': [datetime.time(i).strftime('%H:%M') for i in range(1)]
        }

        # Call the general weather download function with wind specific parameters
        weather(data_package, variable, dates, product_type, file)

    else:
        print('{} already exists. Download is skipped.'.format(file))

    clear_output(wait=False)
    print("Download successful")


def temperatures(input_path, year_start, year_end, test_mode):

    for year in ["%.2d" % y for y in range(year_start, year_end+1)]:
        for variable in ['2m_temperature', 'soil_temperature_level_1']:
            filename = 'ERA_temperature_{}_{}.nc'.format(variable, year)

            # TODO remove
            if test_mode:
                filename = 'ERA_temperature_{}_{}.nc'.format(variable, "2008")

            weather_path = os.path.join(input_path, 'weather')
            os.makedirs(weather_path, exist_ok=True)
            file = os.path.join(weather_path, filename)

            if not os.path.isfile(file):
                #Select period
                data_package = 'reanalysis-era5-single-levels'
                variable = variable
                product_type = 'reanalysis'
                dates = {
                'year': year,
                'month': ["%.2d" % month for month in range(1, 13)],
                'day':  ["%.2d" % day for day in range(1, 32)],
                'time': [datetime.time(i).strftime('%H:%M') for i in range(24)]
                }

                # Call the general weather download function with temperature specific parameters
                weather(data_package, variable, dates, product_type, file)

            else:
                print('{} already exists. Download is skipped.'.format(file))

    clear_output(wait=False)
    print("Download successful")

def weather(data_package, variable, dates, product_type, file):

    # if not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
    #   ssl._create_default_https_context = ssl._create_unverified_context

    c = cdsapi.Client()

    params = {
        'format': 'netcdf',
        'variable': variable,
        "year": dates["year"],
        "month": dates["month"],
        "time": dates["time"],
        'product_type': product_type,
        'area': [72, -10.5, 36.75, 25.5]
    }

    if (variable == '2m_temperature') | (variable == 'soil_temperature_level_1'):
        params["day"] = ["%.2d" % day for day in range(1, 32)]
    c.retrieve(data_package, params, file)


def population(input_path):

    # Set URL and directories
    url = 'https://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/GEOSTAT-grid-POP-1K-2011-V2-0-1.zip'
    population_path = os.path.join(input_path, 'population')
    os.makedirs(population_path, exist_ok=True)
    destination = os.path.join(population_path, 'GEOSTAT-grid-POP-1K-2011-V2-0-1.zip')
    unzip_dir = os.path.join(population_path, 'Version 2_0_1')

    # Download file
    if not os.path.isfile(destination):
        urllib.request.urlretrieve(url, destination)
    else:
        print('{} already exists. Download is skipped.'.format(destination))
    # Unzip file
    if not os.path.isdir(unzip_dir):
        with zipfile.ZipFile(destination, 'r') as f:
            f.extractall(population_path)
    else:
        print('{} already exists. Unzipping is skipped.'.format(unzip_dir))

    clear_output(wait=False)
    print("Download successful")
