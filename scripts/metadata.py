
import json
import yaml
import os
import hashlib
import shutil


# First YAML are defined, which are then parsed and stitched together in the function below

metadata_head = head = '''
hide: yes
external: true
profile: tabular-data-package
name: when2heat
title: when2heat
description: Heat demand and COP time series
long_description: 
    This dataset comprises national time series for representing building heat pumps in power system models. 
    The heat demand of buildins and the coefficient of performance (COP) of heat pumps is calculated 
    for 16 European countries from 2008 to 2013 in an hourly resolution.  
    
    Heat demand time series for space and water heating are computed by combining gas standard 
    load profiles with spatial temperature and wind speed reanalysis data, population geodata, 
    and annual statistics on the final energy consumption for heating. 
    
    COP time series for different heat sources – air, ground, and groundwater – and different 
    heat sinks – floor heating and radiators, both combined with water heating – are calculated 
    based on COP and heating curves, reanalysis temperature data, a spatial aggregation procedure 
    with respect to the heat demand, and a correction procedure for part-load losses.
    
    All data processing as well as the download of relevant input data is conducted in python 
    and pandas and has been documented in the Jupyter notebooks linked below.
homepage: https://data.open-power-system-data.org/when2heat/{version}
documentation: 'https://github.com/oruhnau/when2heat/blob/{version}/main.ipynb'
version: '{version}'
created: '{version}'
last_changes: '{changes}'
keywords:
  - Open Power System Data
  - when2heat
  - time series
  - power systems
  - building heat
  - space heating
  - water heating
  - heat pumps
  - coefficient of performance
geographical-scope: 16 European countries
temporal-scope:
    start: '{start}-01-01'
    end: '{end}-12-31'
contributor:
    name: Oliver Ruhnau
    email: oliver.ruhnau@rwth-aachen.de
    role: author
    organization: RWTH Aachen
sources:
  - name: ECMWF
    web: https://www.ecmwf.int/en/forecasts/datasets/archive-datasets/reanalysis-datasets/era-interim
  - name: Eurostat
    web: http://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/geostat
  - name: EU Building Database
    web: https://ec.europa.eu/energy/en/eu-buildings-database
  - name: BGW
    web: http://www.gwb-netz.de/wa_files/05_bgw_leitfaden_lastprofile_56550.pdf
  - name: BDEW
    web: https://www.enwg-veroeffentlichungen.de/badtoelz/Netze/Gasnetz/Netzbeschreibung/LF-Abwicklung-von-Standardlastprofilen-Gas-20110630-final.pdf
resources:
'''

excel_resource = '''
name: when2heat
title: when2heat excel multiindex
path: when2heat_multiindex.xlsx
format: xlsx
bytes: {bytes}
hash: {hash}
mediatype: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
'''

csv_resource = '''
name: when2heat
title: when2heat csv singleindex
path: when2heat_singleindex.csv
format: csv
bytes: {bytes}
hash: {hash}
profile: tabular-data-resource
mediatype: text/csv
encoding: UTF8
dialect: 
    csvddfVersion: 1.0
    delimiter: ","
    lineTerminator: "\\n" 
    header: true
alternative_formats:
  - path: when2heat_singleindex.csv
    stacking: Singleindex
    format: csv
  - path: when2heat_multiindex.xlsx
    stacking: Multiindex
    format: xlsx
  - path: when2heat_multiindex.csv
    stacking: Multiindex
    format: csv
  - path: when2heat_stacked.csv
    stacking: Stacked
    format: csv
schema:
    primaryKey: utc_timestamp
    missingValues: ""
    fields:
      - name: utc_timestamp
        description: Start of timeperiod in Coordinated Universal Time
        type: datetime
        format: YYYY-MM-DDThh:mm:ssZ
        opsd-contentfilter: true
      - name: cet_cest_timestamp
        description: Start of timeperiod in Central European (Summer-) Time
        type: datetime
        format: YYYY-MM-DDThh:mm:ss
'''

field = '''
name: {country}_{variable}_{attribute}
description: {description}
type: number
unit: {unit}
opsd-properties: 
    country: {country}
    variable: {variable}
    attribute: {attribute}
'''

descriptions = '''
total: Heat demand in {country} in {unit} for space and water heating
space: Heat demand in {country} in {unit} for space heating
water: Heat demand in {country} in {unit} for water heating
ASHP_floor: COP of air-sourced heat pumps (ASHP) for space and water heating in {country} with floor heating
ASHP_radiator: COP of air-sourced heat pumps (ASHP) for space and water heating in {country} with radiator heating
GSHP_floor: COP of ground-sourced heat pumps (GSHP) for space and water heating in {country} with floor heating
GSHP_radiator: COP of ground-sourced heat pumps (GSHP) for space and water heating in {country} with radiator heating
WSHP_floor: COP of groundwater-sourced heat pumps (WSHP) for space and water heating in {country} with floor heating
WSHP_radiator: COP of groundwater-sourced heat pumps (WSHP) for space and water heating in {country} with radiator heating
'''

country_map = {
    'AT': 'Austria',
    'BE': 'Belgium',
    'BG': 'Bulgaria',
    'CZ': 'Czech Republic',
    'DE': 'Germany',
    'FR': 'France',
    'GB': 'Great Britain',
    'HR': 'Croatia',
    'HU': 'Hungary',
    'IE': 'Ireland',
    'NL': 'Netherlands',
    'PL': 'Poland',
    'RO': 'Romania',
    'SI': 'Slovenia',
    'SK': 'Slovakia',
}


def make_json(shaped_dfs, version, changes, year_start, year_end, output_path):

    # Header
    metadata = yaml.load(
        metadata_head.format(version=version, changes=changes, start=year_start, end=year_end)
    )

    # List of resources (files included in the datapackage)
    metadata['resources'] = [
        get_resource(excel_resource, os.path.join(output_path, 'when2heat_multiindex.xlsx')),
        get_resource(csv_resource, os.path.join(output_path, 'when2heat_singleindex.csv'))
    ]

    # List of fields
    for column in shaped_dfs['multiindex'].columns:
        metadata['resources'][1]['schema']['fields'].append(get_field(column))

    # Write the metadata to disk
    with open(os.path.join(output_path, 'datapackage.json'), 'w') as f:
        f.write(
            json.dumps(metadata, indent=4, separators=(',', ': '))
        )


def get_resource(template, file_path):

    file_size = os.path.getsize(file_path)

    with open(file_path, 'rb') as f:
        file_hash = hashlib.md5(f.read()).hexdigest()

    return yaml.load(
        template.format(bytes=file_size, hash=file_hash)
    )


def get_field(column):

    country, variable, attribute, unit = column

    description = yaml.load(
        descriptions.format(country=country_map[country], unit=unit)
    )[attribute]

    return yaml.load(
        field.format(
            country=country,
            variable=variable,
            attribute=attribute,
            unit=unit,
            description=description
        )
    )


def checksums(output_path, home_path):

    os.chdir(output_path)
    files = os.listdir(output_path)

    # Create checksums.txt in the output directory
    with open('checksums.txt', 'w') as f:
        for file_name in files:
            if file_name.split('.')[-1] in ['csv', 'sqlite', 'xlsx']:
                with open(file_name, 'rb') as fx:
                    file_hash = hashlib.md5(fx.read()).hexdigest()
                f.write('{},{}\n'.format(file_name, file_hash))

    # Copy the file to root directory from where it will be pushed to GitHub,
    # leaving a copy in the version directory for reference
    shutil.copyfile('checksums.txt', os.path.join(home_path, 'checksums.txt'))
