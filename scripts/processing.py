# Python modules
import os

from scripts import prepare_data

calculate_safe = False
preproc = False
hourly_calc = False
spatial = False

# %% 1. Settings

version = '2024-02-21'
changes = 'Update and extension'

home_path = os.path.realpath('..')

input_path = os.path.join(home_path, 'input')
interim_path_static = os.path.join(home_path, 'interim_static')
interim_path_yearly = os.path.join(home_path, 'interim_yearly')
interim_path_cop_results = os.path.join(home_path, 'interim_cop_results')
interim_path_heat_results = os.path.join(home_path, 'interim_heat_results')
output_path = os.path.join(home_path, 'output', version)

for path in [input_path, interim_path_static, interim_path_yearly, output_path]:
    os.makedirs(path, exist_ok=True)

all_countries = ['AT', 'BE', 'BG', 'CZ', 'CH', 'DE', 'DK',
                 'EE', 'ES', 'FI', 'FR', 'GB', 'GR', 'HR',
                 'HU', 'IE', 'IT', 'LT', 'LU', 'LV', 'NL',
                 'NO', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK']  # available
countries = ['EE', 'ES', 'FI', 'FR', 'GB']  # all_countries # selected for calculation

year_start = 2008
year_end = 2014

# As the CDS data download currently takes days to weeks for multiple years, a dummy mode is created to allow testing
# with multiple time horizons. with multiple time horizons. This mode only needs the year 2008 and calculates
# artificial periods for the desired time period. Leap years are also taken into account, so a complete test is
# possible. https://forum.ecmwf.int/t/a-new-cds-soon-to-be-launched-expect-some-disruptions/1607
# TODO remove test_mode and recalculate_inputs
test_mode = True
recalculate_inputs = False

# %% 2. Prepare data and write results
for year in range(year_start, year_end + 1):
    if not prepare_data.check_exist_final_data(interim_path_cop_results,
                                               interim_path_heat_results, year):
        # calculate inout data only in first iteration
        if year == year_start or recalculate_inputs:
            print('prepare_overall_input year ')
            prepare_data.prepare_overall_input(input_path, countries, interim_path_static)
            print('Prepare_yearly_input year ')
            prepare_data.prepare_yearly_input(input_path, year, year_end, test_mode, interim_path_yearly,
                                              interim_path_static)

        # calculate final_cop and final_heat for every year in the range
        print('calculate_daily_demand year ' + str(year))
        prepare_data.calculate_daily_demand(interim_path_yearly, interim_path_static, year)
        print('calculate_hourly_demand year ' + str(year))
        prepare_data.calculate_hourly_demand_(input_path, interim_path_yearly, interim_path_static, year)
        print('weight_scale year ' + str(year))
        prepare_data.weight_scale(input_path, interim_path_yearly, interim_path_static, interim_path_heat_results, year)
        print('aggregate_and_combine year ' + str(year))
        prepare_data.aggregate_and_combine(input_path, interim_path_yearly, interim_path_cop_results, year, countries)
        print('Calculations ready year ' + str(year))
        prepare_data.free_up_space(interim_path_yearly, year)
        print('Space freed up year ' + str(year))
        print('-------------------------------------')
    else:
        print('Final ' + str(year) + ' already exists')

# combine yearly data and write results to various data formats
prepare_data.write_results(input_path, interim_path_heat_results, interim_path_cop_results, output_path, home_path,
                           version, changes, year_start, year_end)
