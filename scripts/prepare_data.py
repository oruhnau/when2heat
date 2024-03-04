import gc
import os
import pickle
import re
import shutil

import numpy as np
import pandas as pd

from scripts import download, preprocess, read, demand, write, cop, metadata


def prepare_overall_input(input_path, countries, interim_path):
    """
    Prepare yearly independent data for all selected countries and save the interim results.

    :param input_path: Path to the input data.
    :param countries: List of countries for which data will be prepared.
    :param interim_path: Path to store interim results.

    This function prepares yearly independent data for all selected countries and saves the interim results
    for further analysis. It downloads wind and population data, preprocesses them, and saves mapped population,
    wind data, heating thresholds, daily parameters, and building database to the interim path.
    """
    # download data
    download.wind(input_path)
    download.population(input_path)

    # preprocessing
    mapped_population = preprocess.map_population(input_path, countries, interim_path)
    with open((interim_path + '/mapped_population.pkl'), 'wb') as handle:
        pickle.dump(mapped_population, handle, protocol=pickle.HIGHEST_PROTOCOL)

    wind = preprocess.wind(input_path, mapped_population)
    wind.to_pickle(os.path.join(interim_path, "wind"))

    heating_thresholds = read.heating_thresholds(input_path)
    heating_thresholds.to_pickle(os.path.join(interim_path, "heating_thresholds.pkl"))

    daily_parameters = read.daily_parameters(input_path)
    daily_parameters.to_pickle(os.path.join(interim_path, "daily_parameters.pkl"))

    building_database = read.building_database(input_path)
    with open((interim_path + '/building_database.pkl'), 'wb') as handle:
        pickle.dump(building_database, handle, protocol=pickle.HIGHEST_PROTOCOL)


# TODO remove test mode!
def prepare_yearly_input(input_path, year_start, year_end, test_mode, interim_path_yearly, interim_path_static):
    """
    Prepare yearly dependent data for all selected countries and save the interim results

    :param input_path: Path to the input data.
    :param year_start: Starting year for data preparation.
    :param year_end: Ending year for data preparation.
    :param test_mode: Boolean indicating whether the function is running in test mode.
    :param interim_path_yearly: Path to store interim yearly results.
    :param interim_path_static: Path to static interim data.

    This function prepares yearly input data for the heat demand calculations. It downloads temperature data,
    preprocesses it, calculates reference temperature, adjusts temperature based on heating thresholds,
    and stores the prepared data for further analysis.
    """
    heating_thresholds = pd.read_pickle(os.path.join(interim_path_static, 'heating_thresholds.pkl'))
    with open((interim_path_static + '/mapped_population.pkl'), 'rb') as handle:
        mapped_population = pickle.load(handle)

    for year_start in range(year_start, year_end + 1):
        # download data
        # TODO check whether it it more efficient to download all years at once
        download.temperatures(input_path, year_start, year_end, test_mode)

        # TODO check implementation with 2x year_start
        temperature = preprocess.temperature(input_path, year_start, year_start, mapped_population, test_mode)
        temperature.to_pickle(os.path.join(interim_path_yearly, "temperature" + str(year_start)[-2:] + '.pkl'))

        # heat demand time series
        reference_temperature = demand.reference_temperature(temperature['air'])
        reference_temperature.to_pickle(
            os.path.join(interim_path_yearly, "reference_temperature") + str(year_start)[-2:] + '.pkl')

        adjusted_temperature = demand.adjust_temperature(reference_temperature, heating_thresholds)
        adjusted_temperature.to_pickle(
            os.path.join(interim_path_yearly, "adjusted_temperature") + str(year_start)[-2:] + '.pkl')


def calculate_daily_demand(interim_path_yearly, interim_path_static, year):
    """
    Calculate and store daily demand for heat and water.

    :param interim_path_yearly: The directory path where yearly interim results are stored.
    :param interim_path_static: The directory path where static interim data is stored.
    :param year: The year for which the demand needs to be calculated.

    This function reads adjusted temperature and wind data from the given directories,
    calculates daily heat and water demand using the provided parameters, and stores the results
    as pickled files in the yearly interim directory.
    """
    # read data from interim results
    adjusted_temperature = pd.read_pickle(
        os.path.join(interim_path_yearly, 'adjusted_temperature') + str(year)[-2:] + '.pkl')
    daily_parameters = pd.read_pickle(os.path.join(interim_path_static, 'daily_parameters') + '.pkl')
    wind = pd.read_pickle(os.path.join(interim_path_static, 'wind'))

    # calculate and store heat demand (daily)
    daily_heat = demand.daily_heat(adjusted_temperature,
                                   wind,
                                   daily_parameters)
    daily_heat.to_pickle(os.path.join(interim_path_yearly, "daily_heat") + str(year)[-2:] + '.pkl')

    # calculate and store water demand (daily)
    daily_water = demand.daily_water(adjusted_temperature,
                                     wind,
                                     daily_parameters)
    daily_water.to_pickle(os.path.join(interim_path_yearly, "daily_water") + str(year)[-2:] + '.pkl')


def calculate_hourly_demand_(input_path, interim_path_yearly, interim_path_static, year):
    """
    Calculate and store hourly demand for heat and water.

    :param input_path: Path to input data.
    :param interim_path_yearly: Path to interim yearly results.
    :param interim_path_static: Path to static interim data.
    :param year: The year for which the demand needs to be calculated.

    This function reads reference temperature and daily heat/water from the given directories,
    calculates hourly heat and water demand using the provided parameters, and stores the results
    as pickled files in the yearly interim directory.
    """
    # read data from interim results
    daily_heat = pd.read_pickle(os.path.join(interim_path_yearly, 'daily_heat') + str(year)[-2:] + '.pkl')
    reference_temperature = pd.read_pickle(
        os.path.join(interim_path_yearly, 'reference_temperature') + str(year)[-2:] + '.pkl')
    daily_water = pd.read_pickle(os.path.join(interim_path_yearly, 'daily_water') + str(year)[-2:] + '.pkl')
    hourly_parameters = read.hourly_parameters(input_path)

    # calculate and store heat demand (hourly)
    hourly_heat = demand.hourly_heat(daily_heat,
                                     reference_temperature,
                                     hourly_parameters)
    hourly_heat.to_pickle(os.path.join(interim_path_yearly, "hourly_heat") + str(year)[-2:] + '.pkl')
    del hourly_heat, daily_heat
    gc.collect()

    # calculate and store water demand (hourly)
    hourly_water = demand.hourly_water(daily_water,
                                       reference_temperature,
                                       hourly_parameters)
    hourly_water.to_pickle(os.path.join(interim_path_yearly, "hourly_water") + str(year)[-2:] + '.pkl')

    # combine heat and water data
    hourly_heat = pd.read_pickle(os.path.join(interim_path_yearly, 'hourly_heat') + str(year)[-2:] + '.pkl')
    hourly_space = (hourly_heat - hourly_water).clip(lower=0)
    hourly_space.to_pickle(os.path.join(interim_path_yearly, "hourly_space") + str(year)[-2:] + '.pkl')


def check_exist_final_data(interim_path_cop_results,
                           interim_path_heat_results, year):
    """
    Check if both cop and heat results exist.

    :param interim_path_cop_results: Path to the directory containing COP results.
    :param interim_path_heat_results: Path to the directory containing heat results.
    :param year: The year for which the data existence needs to be checked.
    :return: True if both final heat and COP data files exist, False otherwise.

    This function checks whether the final heat and COP data files for a specific year exist. It constructs the file
    paths based on the provided interim paths and the given year. If both files exist, it returns True; otherwise,
    it returns False.
    """
    file_path_heat = os.path.join(interim_path_heat_results, 'final_heat' + str(year)[-2:] + '.pkl')
    file_path_cop = os.path.join(interim_path_cop_results, 'final_cop' + str(year)[-2:] + '.pkl')

    # if both files exists return true
    if os.path.exists(file_path_heat) and os.path.exists(file_path_cop):
        return True
    else:
        return False


def weight_scale(input_path, interim_path_yearly, interim_path_static, interim_path_heat_results, year):
    """
    Calculate and store weighted spatial demand for heat and water.

    :param input_path: Path to the input data.
    :param interim_path_yearly: Path to store interim yearly results.
    :param interim_path_static: Path to static interim data.
    :param interim_path_heat_results: Path to store heat results.
    :param year: The year for which the calculations are performed.

    This function calculates the weighted spatial demand for heat based on the provided input data. It reads
    necessary data such as mapped population, building database, and hourly space and water demand. Then, it finishes
    and stores spatial demand for both space heating and water heating. Finally, it aggregates and combines the
    spatial demand and stores the interim results.
    """

    # read required data
    with open((interim_path_static + '/mapped_population.pkl'), 'rb') as handle:
        mapped_population = pickle.load(handle)
    building_database = read.building_database(input_path)
    hourly_space = pd.read_pickle(os.path.join(interim_path_yearly, 'hourly_space') + str(year)[-2:] + '.pkl')

    # finish and store spatial demand (hourly)
    spatial_space = demand.finishing(hourly_space, mapped_population, building_database['space'])
    del hourly_space
    gc.collect()

    # finish and store spatial water demand (hourly)
    hourly_water = pd.read_pickle(os.path.join(interim_path_yearly, 'hourly_water') + str(year)[-2:] + '.pkl')
    spatial_water = demand.finishing(hourly_water, mapped_population, building_database['water'])
    del mapped_population, building_database
    gc.collect()

    # aggregate and combine
    final_heat = demand.combine(spatial_space, spatial_water)

    # store interim results
    final_heat.to_pickle(os.path.join(interim_path_heat_results, 'final_heat' + str(year)[-2:] + '.pkl'))
    spatial_space.to_pickle(os.path.join(interim_path_yearly, 'spatial_space_' + str(year)[-2:] + '.pkl'))
    spatial_water.to_pickle(os.path.join(interim_path_yearly, 'spatial_water_' + str(year)[-2:] + '.pkl'))


def aggregate_and_combine(input_path, interim_path_yearly, interim_path_cop_results,
                          year, countries):
    """
    Aggregate and combine spatial data for selected countries.

    :param input_path: Path to the input data.
    :param interim_path_yearly: Path to interim yearly results.
    :param interim_path_cop_results: Path to store COP results.
    :param year: The year for which the calculations are performed.
    :param countries: List of countries to include in the aggregation.

    This function aggregates and combines spatial data for selected countries based on the input data. It reads
    temperature data, calculates source and sink temperatures, and computes spatial using parameters. Then,
    it reads spatial space and water demand data, and finally, it combines the data with spatial demand data
    to generate final COP results, which are saved in the specified interim path.
    """
    # COP time series
    temperature = pd.read_pickle(os.path.join(interim_path_yearly, 'temperature' + str(year)[-2:] + '.pkl'))
    source_temperature = cop.source_temperature(temperature)
    sink_temperature = cop.sink_temperature(temperature)

    # calculate spatial COP
    cop_parameters = read.cop_parameters(input_path)
    spatial_cop = cop.spatial_cop(source_temperature, sink_temperature, cop_parameters)
    del source_temperature, sink_temperature, cop_parameters, temperature
    gc.collect()

    spatial_space = pd.read_pickle(os.path.join(interim_path_yearly, 'spatial_space_' + str(year)[-2:] + '.pkl'))[
        countries]
    spatial_water = pd.read_pickle(os.path.join(interim_path_yearly, 'spatial_water_' + str(year)[-2:] + '.pkl'))[
        countries]

    # calculate final cop
    final_cop = cop.finishing(spatial_cop, spatial_space, spatial_water)
    final_cop.to_pickle(os.path.join(interim_path_cop_results, 'final_cop' + str(year)[-2:] + '.pkl'))


def free_up_space(interim_path_yearly, year_start):
    """
    Remove interim yearly files for a specific year to free up space.

    :param interim_path_yearly: Path to the directory containing interim yearly files.
    :param year_start: The year for which files need to be removed.
    """
    list_of_files = [file for file in os.listdir(interim_path_yearly)
                     if extract_year(file) is not None and year_start % 100 == extract_year(file)]
    for file in list_of_files:
        os.remove(os.path.join(interim_path_yearly, file))


# Function to extract the year from the filename
def extract_year(filename):
    """
    Extract the year from a filename if it contains the two-digit year representation.

    :param filename: The filename from which to extract the year.
    :return: The extracted year as an integer if found, otherwise None.
    """
    match = re.search(r'\d{2}', filename)
    if match:
        return int(match.group())
    return None


def combine_data(interim_path_results, year_start, year_end):
    """
    Combine and merge data from interim results for a specified year range.

    :param interim_path_results: Path to the directory containing interim results.
    :param year_start: The starting year of the range (inclusive).
    :param year_end: The ending year of the range (inclusive).
    :return: Combined and merged DataFrame containing data for the specified year range.

    This function combines and merges data from interim results stored as pickle files within the specified directory.
    It filters files based on the provided year range, concatenates them into a single DataFrame, and sorts the index.
    Then, it merges duplicate indices (resulting from yearly calculations) by summing them up and filling missing values.
    The resulting DataFrame contains aggregated data for the specified year range.
    """

    # filter files based on the year range
    list_of_files = [file for file in os.listdir(interim_path_results)
                     if extract_year(file) is not None and year_start % 100 <= extract_year(file) <= year_end % 100]

    # concat files between year_start and year_end
    final_df = pd.concat(
        [pd.read_pickle(os.path.join(interim_path_results, f), compression=None) for f in list_of_files if
         f.endswith('.pkl')], sort=False)
    final_df.sort_index(inplace=True)

    # merge duplicate index into single index (result due to the yearly calculations)
    final_df_merge = final_df.groupby(level=0).sum()
    final_df_merge = final_df_merge.replace(0, np.nan).ffill()
    final_df_merge = final_df_merge.replace(0, np.nan).bfill()

    return final_df_merge


def write_results(input_path, interim_path_heat_results, interim_path_cop_results, output_path, home_path, version,
                  changes, year_start, year_end):
    """
    Write final results based on interim heat and COP results.

    :param input_path: Path where the original input data is stored.
    :param interim_path_heat_results: Path where the interim results for heat are stored.
    :param interim_path_cop_results: Path where the interim results for COP are stored.
    :param output_path: Path where the output is stored.
    :param home_path: Path of the project home folder.
    :param version: Name of the version.
    :param changes: Description of the changes.
    :param year_start: The starting year of the data.
    :param year_end: The ending year of the data.

    This function combines and writes final results based on interim heat and COP results. It combines heat and COP data
    for the specified year range, shapes the resulting DataFrames, writes them to SQL and CSV formats, generates
    metadata in JSON format, and copies the original input data to the output directory. Finally, it computes checksums
    for the output data and prints a message indicating that the results are ready.
    """

    # combine data sets
    final_heat = combine_data(interim_path_heat_results, year_start, year_end)
    final_cop = combine_data(interim_path_cop_results, year_start, year_end)

    # write DFs in the desired format
    shaped_dfs = write.shaping(final_heat, final_cop)
    del final_cop, final_heat
    gc.collect()

    # store data
    write.to_sql(shaped_dfs, output_path, home_path)
    write.to_csv(shaped_dfs, output_path)
    metadata.make_json(shaped_dfs, version, changes, year_start, year_end, output_path)

    # copy original input data to output directory
    shutil.copytree(input_path, os.path.join(output_path, 'original_data'))

    metadata.checksums(output_path, home_path)
    # final_cop = final_cop.fillna(method='bfill').fillna(method='ffill').groupby(final_cop.index).mean()
    print('results ready')
