
import json
import yaml
import os
import hashlib
import shutil


# First YAML are defined, which are then parsed and stitched together in the function below


metadata_head = head = '''
name: when2heat
external: true
id: https://doi.org/10.25832/when2heat/{version}
profile: tabular-data-package
licenses:
  - name: cc-by-4.0
    title: Creative Commons Attribution 4.0
    path: https://creativecommons.org/licenses/by/4.0/
attribution: 
    "Attribution should be given as follows: <ul>
    <li style='margin-bottom: 0;'>Ruhnau, O., Hirth, L., and Praktiknjo, A. (2019). Time series of heat demand and heat pump efficiency for energy system modeling. Scientific Data, 6, 189. 
    <a href='https://doi.org/10.1038/s41597-019-0199-y'>https://doi.org/10.1038/s41597-019-0199-y</a>
    <li style='margin-bottom: 0;'>Ruhnau, O., Muessel, J. ({year}). Update and extension of the When2Heat dataset. Econstor Working Paper. 
    <a href='http://hdl.handle.net/10419/249997'>http://hdl.handle.net/10419/249997</a>
    <li style='margin-bottom: 0;'>Ruhnau, O., Muessel, J. ({year}). When2Heat Heating Profiles. Open Power System Data. 
    <a href='https://doi.org/10.25832/when2heat/{version}'>https://doi.org/10.25832/when2heat/{version}</a>
    </ul>"  
title: When2Heat Heating Profiles
description: Simulated hourly country-aggregated heat demand and COP time series
homepage: https://data.open-power-system-data.org/when2heat/{version}
version: '{version}'
sources:
  - title: ECMWF
    web: https://www.ecmwf.int/en/forecasts/datasets/reanalysis-datasets/era5
  - title: Eurostat
    web: http://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/population-distribution-demography/geostat
  - title: JRC-IDEEES
    web: https://ec.europa.eu/jrc/en/potencia/jrc-idees
  - title: BGW
    web: http://www.gwb-netz.de/wa_files/05_bgw_leitfaden_lastprofile_56550.pdf
    description: Data from pages 55 and 85f are used.
  - title: BDEW
    web: https://www.enwg-veroeffentlichungen.de/badtoelz/Netze/Gasnetz/Netzbeschreibung/LF-Abwicklung-von-Standardlastprofilen-Gas-20110630-final.pdf
contributors:
  - name: Oliver Ruhnau
    email: ruhnau@hertie-school.org
    role: author
    organization: Hertie School
  - name: Jarusch Muessel
    email: muessel@hertie-school.org
    role: author
    organization: Hertie School
    
lastChanges: '{changes}'
keywords:
  - Open Power System Data
  - When2Heat
  - heating profiles
  - time series
  - power systems
  - building heat
  - space heating
  - water heating
  - heat pumps
  - coefficient of performance
publicationDate: '{version}'
longDescription: 
    This dataset comprises national time series for representing building heat pumps in power system models. 
    The heat demand of buildings and the coefficient of performance (COP) of heat pumps is calculated 
    for 28 European countries from {start} to {end} in an hourly resolution.  
    
    Heat demand time series for space and water heating are computed by combining gas standard 
    load profiles with spatial temperature and wind speed reanalysis data as well as population geodata.  
    The profiles are year-wise scaled to 1 TWh each. For the years 2008 to 2015, the data is additionally 
    scaled with annual statistics on the final energy consumption for heating.
    
    COP time series for different heat sources – air, ground, and groundwater – and different heat sinks 
    – floor heating, radiators, and water heating – are calculated based on COP and heating curves 
    using reanalysis temperature data, spatially aggregated with respect to the heat demand, 
    and corrected based on field measurements.
    
    All data processing as well as the download of relevant input data is conducted in python 
    and pandas and has been documented in the Jupyter notebooks linked below. Please also consider and cite
    our <a href="https://doi.org/10.1038/s41597-019-0199-y">Data Descriptor</a> of the original dataset as well as 
    our <a href="http://hdl.handle.net/10419/249997">Working Paper</a> at on recent updates and extensions of the dataset.
documentation: 'https://github.com/oruhnau/when2heat/blob/{version}/main.ipynb'
spatial:
   location: 28 European countries
   resolution: Countries
_external: true
temporal:
    start: '{start}-01-01'
    end: '{end}-12-31'
    resolution: hourly
_metadataVersion: 1.2
resources:
'''

excel_resource = '''
name: when2heat
path: when2heat.xlsx
title: When2Heat excel multiindex
format: xlsx
bytes: {bytes}
hash: {hash}
mediatype: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
'''

csv_resource = '''
name: when2heat
profile: tabular-data-resource
path: when2heat.csv
title: When2heat csv singleindex
format: csv
mediatype: text/csv
encoding: UTF8
bytes: {bytes}
hash: {hash}
dialect: 
    delimiter: ","
    decimalChar: "."
    lineTerminator: "\\n" 
    header: true
_alternativeFormats:
  - path: when2heat.csv
    stacking: Singleindex
    format: csv
  - path: when2heat.xlsx
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
        opsdContentfilter: true
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
opsdContentfilter: false
opsdProperties: 
    country: {country}
    variable: {variable}
    attribute: {attribute}
'''

descriptions = '''
heat_demand:
    total: Heat demand in {country} in {unit} for space and water heating
    space: Heat demand in {country} in {unit} for space heating
    water: Heat demand in {country} in {unit} for water heating
    space_SFH: Heat demand in {country} in {unit} for space heating in single-family houses
    space_MFH: Heat demand in {country} in {unit} for space heating in multi-family houses
    space_COM: Heat demand in {country} in {unit} for space heating in commercial buildings
    water_SFH: Heat demand in {country} in {unit} for water heating in single-family houses
    water_MFH: Heat demand in {country} in {unit} for water heating in multi-family houses
    water_COM: Heat demand in {country} in {unit} for water heating in commercial buildings
heat_profile:
    space_SFH: Normalized heat demand in {country} in {unit} for space heating in single-family houses
    space_MFH: Normalized heat demand in {country} in {unit} for space heating in multi-family houses
    space_COM: Normalized heat demand in {country} in {unit} for space heating in commercial buildings
    water_SFH: Normalized heat demand in {country} in {unit} for water heating in single-family houses
    water_MFH: Normalized heat demand in {country} in {unit} for water heating in multi-family houses
    water_COM: Normalized heat demand in {country} in {unit} for water heating in commercial buildings
COP:
    ASHP_floor: COP of air-source heat pumps (ASHP) for space heating in {country} with floor heating
    ASHP_radiator: COP of air-source heat pumps (ASHP) for space heating in {country} with radiators
    ASHP_water: COP of air-source heat pumps (ASHP) for water heating in {country}
    GSHP_floor: COP of ground-source heat pumps (GSHP) for space heating in {country} with floor heating
    GSHP_radiator: COP of ground-source heat pumps (GSHP) for space heating in {country} with radiators
    GSHP_water: COP of ground-source heat pumps (GSHP) for water heating in {country}
    WSHP_floor: COP of groundwater-source heat pumps (WSHP) for space heating in {country} with floor heating
    WSHP_radiator: COP of groundwater-source heat pumps (WSHP) for space heating in {country} with radiators
    WSHP_water: COP of groundwater-source heat pumps (WSHP) for water heating in {country}
'''

country_map = {
    'AT': 'Austria',
    'BE': 'Belgium',
    'BG': 'Bulgaria',
    'CH': 'Switzerland',
    'CZ': 'Czech Republic',
    'DE': 'Germany',
    'DK': 'Denmark',
    'EE': 'Estonia',
    'ES': 'Spain',
    'FI': 'Finland',
    'FR': 'France',
    'GB': 'Great Britain',
    'GR': 'Greece',
    'HR': 'Croatia',
    'HU': 'Hungary',
    'IE': 'Ireland',
    'IT': 'Italy',
    'LT': 'Lithuania',
    'LU': 'Luxembourg',
    'LV': 'Latvia',
    'NL': 'Netherlands',
    'NO': 'Norway',
    'PL': 'Poland',
    'PT': 'Portugal',
    'RO': 'Romania',
    'SE': 'Sweden',
    'SI': 'Slovenia',
    'SK': 'Slovakia',
}


def make_json(shaped_dfs, version, changes, year_start, year_end, output_path):

    # Header
    metadata = yaml.load(
        metadata_head.format(version=version, changes=changes, start=year_start, end=year_end, year=version[:4]),
        Loader=yaml.SafeLoader
    )

    # List of resources (files included in the datapackage)
    metadata['resources'] = [
        get_resource(excel_resource, os.path.join(output_path, 'when2heat.xlsx')),
        get_resource(csv_resource, os.path.join(output_path, 'when2heat.csv'))
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
        template.format(bytes=file_size, hash=file_hash),  Loader=yaml.SafeLoader
    )


def get_field(column):

    country, variable, attribute, unit = column

    description = yaml.load(
        descriptions.format(country=country_map[country], unit=unit), Loader=yaml.SafeLoader
    )[variable][attribute]

    return yaml.load(
        field.format(
            country=country,
            variable=variable,
            attribute=attribute,
            unit=unit,
            description=description
        ), Loader=yaml.SafeLoader
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
