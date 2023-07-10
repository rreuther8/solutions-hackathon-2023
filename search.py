#remove unnecessary imports
import os
import re
import logging
import click

import pandas as pd



SEPARATORS = [',', '|', '\t']
FILE_SEARCH_LOG = 'Seaching for files containing "{}".'
NO_FILE_EXCEPTION = 'No files names containing "{}" keyword.'
NO_COLUMN_EXCEPTION = 'No columns names containing "{}" keyword.'
COLUMN_MATCH_LOG = 'Column Regex Match for "{}":'
NO_SEPARATOR_EXCEPTION = 'No separator found in file.'

#instantiate the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#create a function that finds all file names that match the regex
def get_filename_from_regex(file_regex):
	

	#get all files in the current directory
	files = os.listdir()

	#find strings in a list of strings that match a regex
	regex_files = [file for file in files if re.search(file_regex, file)]
	

	#raise exception if no files are found
	if not regex_files:
		raise ValueError(NO_FILE_EXCEPTION.format(file_regex))
	return regex_files


#read a file and infer the separator
def infer_separator(filename):
	with open(filename, 'r') as f:
		first_line = f.readline()
		for separator in SEPARATORS:
			if separator in first_line:
				return separator
		else:
			raise ValueError(NO_SEPARATOR_EXCEPTION)


def read_files(filenames):
	separator = infer_separator(filenames[0])

	#read all files in a pandas dataframe and include the filename as a column
	df = pd.concat([pd.read_csv(filename, sep=separator).assign(filename=filename) for filename in filenames])

	return df


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
		raise ValueError(NO_COLUMN_EXCEPTION.format(column_regex))

	return groupings_dict


#create a function to take a pandas dataframe, and filter it to ensure that there are no null values in a list of columns.
def filter_null_values(dataframe, columns):
	
	for column in columns:
		filtered_dataframe = df[column].notnull()
	return filters



def get_column_value_regex_matches(dataframe, columns, value_regex):
	df_filtered = filter_null_values(dataframe, columns)
	df_filtered = df.where(F.expr(filters)).persist()

	column_dict = {column: df_filtered.filter(F.col(column).contains(value_regex)).count() for column in columns}
	
	return column_dict




def log(columns, column_regex, column_groupings):
	logger.info(
		'\n'.join([
		COLUMN_MATCH_LOG.format(column_regex),
		'\n\t'.join(columns)
	]))


@click.command()
@click.option("--file_regex", "-f", required=True, default=None)
@click.option("--column_regex", "-c", required=True, default=None)
@click.option("--value_regex", "-v", default=None)
def main(file_regex, column_regex, value_regex):
	try:
		
		
		#log an error message saying "hello"
		logger.info('Hello')

		regex_filenames = get_filename_from_regex(file_regex)
		df = read_files(regex_filenames)

		columns = get_column_regex_matches(df, column_regex)
		column_groupings = get_column_group_counts(df, columns)
		log(columns, column_regex, column_groupings)

		import ipdb; ipdb.set_trace()

		if value_regex:
			column_dict = get_column_value_regex_matches(df, columns, value_regex)
			# LOG
	
	finally:
		logger.error("Goodbye")


if __name__ == "__main__":
	main()

# python search.py -f customer -c rewards_tier -v Gold
