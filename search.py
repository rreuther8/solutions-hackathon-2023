#remove unnecessary imports
import os
import re
import click

import pandas as pd


SEPARATORS = [',', '|', '\t']
SEARCH_LOG = 'Searching for {} containing "{}".'
SEARCH_FINISH_LOG = 'Completed search.'
SEARCH_FAILURE_LOG = 'SEARCH FAILED.'
SEARCH_FAILURE_EXCEPTION = 'No {} containing "{}" keyword.'
MATCH_LOG = 'Located {} regex match for "{}":'


#create a function that finds all file names that match the regex
def get_filename_from_regex(file_regex):
	#get all files in the current directory
	files = os.listdir()

	#find strings in a list of strings that match a regex
	regex_files = [file for file in files if re.search(file_regex, file)]
	

	#raise exception if no files are found
	if not regex_files:
		raise ValueError(SEARCH_FAILURE_EXCEPTION.format('files', file_regex))
	return regex_files


#read a file and infer the separator
def infer_separator(filename):
	with open(filename, 'r') as f:
		first_line = f.readline()
		for separator in SEPARATORS:
			if separator in first_line:
				return separator
		else:
			raise ValueError(SEARCH_FAILURE_EXCEPTION.format('separators', file_regex))


def read_files(filenames):
	separator = infer_separator(filenames[0])

	#read all files in a pandas dataframe and include the filename as a column
	dataframe = pd.concat([pd.read_csv(filename, sep=separator).assign(filename=filename) for filename in filenames])

	if len(dataframe) == 0:
		raise ValueError(SEARCH_FAILURE_EXCEPTION.format('files', column_regex))
	return dataframe


def get_column_regex_matches(df, column_regex):
	columns = [c for c in df.columns if column_regex in c]
	
	#raise exception if columns is empty
	if not columns:
		raise ValueError(NO_COLUMN_EXCEPTION.format(column_regex))

	return columns


def get_column_group_counts(dataframe, columns):
	# in pandas get the distinct counts for each value in a column
	groupings_dict = { column: dataframe[column].value_counts().reset_index(name='counts') for column in columns }
	
	#raise exception if groupings_dict is empty
	if not groupings_dict:
		raise ValueError(SEARCH_FAILURE_EXCEPTION.format('column', column_regex))

	return groupings_dict


#create a function to take a pandas dataframe, and filter it to ensure that there are no null values in a list of columns.
def filter_null_values(dataframe, columns):
	filtered_dataframe = dataframe.copy()

	for column in columns:
		filtered_dataframe = filtered_dataframe[filtered_dataframe[column].notnull()]

	return filtered_dataframe


#write a function filter_for_regex_pattern to take a pandas dataframe, 
#a list of columns, and a regex pattern, and return a dataframe filtered to only include rows where the value in the column matches the regex pattern.
def filter_for_regex_pattern(filtered_df, columns, value_regex):
	value_df = filtered_df.copy()

	for column in columns:
		value_df = value_df[value_df[column].str.contains(value_regex, na=False)]

	return value_df


def get_column_value_regex_matches(dataframe, columns, value_regex):
	filtered_df = filter_null_values(dataframe, columns)

	value_df = filter_for_regex_pattern(filtered_df, columns, value_regex)

	value_dict = get_column_group_counts(value_df, columns)

	#raise exception if value_dict is empty
	if value_dict == 0:
		raise ValueError(NO_COLUMN_EXCEPTION.format(column_regex))

	return value_dict


def log(regex, groupings, level):
	for key, dataframe in groupings.items():
		print(
			'\n\n'.join([
			MATCH_LOG.format(level, regex),
			dataframe.to_string(),
			''
		]))


@click.command()
@click.option("--file_regex", "-f", required=True, default=None)
@click.option("--column_regex", "-c", required=True, default=None)
@click.option("--value_regex", "-v", default=None)
def main(file_regex, column_regex, value_regex):
	try:

		print(SEARCH_LOG.format('files', file_regex))
		regex_filenames = get_filename_from_regex(file_regex)
		dataframe = read_files(regex_filenames)

		filename_dict = get_column_group_counts(dataframe, ['filename'])
		log(file_regex, filename_dict, 'file')

		print(SEARCH_LOG.format('columns', column_regex))
		columns = get_column_regex_matches(dataframe, column_regex)
		column_dict = get_column_group_counts(dataframe, columns)
		log(column_regex, column_dict, 'column')

		if value_regex:
			print(SEARCH_LOG.format('column values', value_regex))
			value_dict = get_column_value_regex_matches(dataframe, columns, value_regex)
			log(value_regex, value_dict, 'column value')

	except:
		print(SEARCH_FAILURE_LOG)
	finally:
		print(SEARCH_FINISH_LOG)


if __name__ == "__main__":
	main()

# python search.py -f customer -c rewards_tier -v Gold
