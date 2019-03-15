
import os
import ssl
import datetime
import urllib
import zipfile
from ecmwfapi import ECMWFDataServer


def wind(input_path):

    filename = 'ERA_wind.nc'
    weather_path = os.path.join(input_path, 'weather')
    os.makedirs(weather_path, exist_ok=True)
    file = os.path.join(weather_path, filename)

    if not os.path.isfile(file):

        # Select all months from 1979 to 2016 by the date of the first day of the month
        dates = "".join([datetime.date(year, month, 1).strftime("%Y%m%d") + "/"
                         for year in range(1979, 2018)
                         for month in [10, 11, 12, 1, 2, 3, 4]])[:-1]

        # Call the general weather download function with wind specific parameters
        weather(date=dates,
                param="207.128",
                stream="mnth",
                target=file)

    else:
        print('{} already exists. Download is skipped.'.format(file))


def temperatures(input_path, year_start, year_end):

    for year in range(year_start, year_end+1):

        filename = 'ERA_temperature_{}.nc'.format(year)
        weather_path = os.path.join(input_path, 'weather')
        os.makedirs(weather_path, exist_ok=True)
        file = os.path.join(weather_path, filename)

        if not os.path.isfile(file):

            # Call the general weather download function with temperature specific parameters
            weather(date="{}-01-01/to/{}-12-31".format(year, year),
                    param="167.128/236.128",
                    stream="oper",
                    target=file)

        else:
            print('{} already exists. Download is skipped.'.format(file))


def weather(date, param, stream, target):

    if not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

    server = ECMWFDataServer()

    params = {
        "date"		: date,
        "param"		: param,
        "stream"	: stream,
        "target"	: target,
        "class"		: "ei",
        "dataset"	: "interim",
        "expver"	: "1",
        "grid"		: "0.75/0.75",
        "levtype"	: "sfc",
        "step"		: "0",
        "time"		: "00:00:00/06:00:00/12:00:00/18:00:00",
        "type"		: "an",
        "area"		: "72/-10.5/36.75/25.5",
        "format"	: "netcdf"
    }

    server.retrieve(params)


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
