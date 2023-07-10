import click

from pyspark.sql import functions as F, Window, types as spark_types

from pandas import pd
from buildamap.utils import spark
from buildamap.common import getlogger
from buildamap.rds.utils.postgres_requests import list_audiences
from buildamap.parse_bam_config import OverriddenConfig, get_bam_config



logger = getlogger(__name__)
SEPARATORS = [',', '|', '\t']

FILE_SEARCH_LOG = 'Seaching for files containing "{}".'
NO_FILE_EXCEPTION = 'No files names containing "{}" keyword.'
NO_COLUMN_EXCEPTION = 'No columns names containing "{}" keyword.'
COLUMN_MATCH_LOG = 'Column Regex Match for "{}":'


#create a function that finds all file names that match the regex
def get_filename_from_regex(file_regex):
	#get all files in the current directory
	files = os.listdir()

	#use the regex to find all files that match the regex
	regex_files = re.findall(file_regex, files)

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
			raise ValueError('No separator found in file.')


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




def concatenate_filters(columns):
	return ' OR '.join(
		["{} IS NOT NULL".format(col) for col in columns]
	)


def get_column_value_regex_matches(df, columns, value_regex):
	filters = concatenate_filters(columns)
	df_filtered = df.where(F.expr(filters)).persist()

	column_dict = {column: df_filtered.filter(F.col(column).contains(value_regex)).count() for column in columns}
	
	return column_dict

def get_column_group_counts(df, columns):
	#from the df pandas dataframe, get grouped counts of the columns individually
	column_groupings = df.groupby(columns).size().reset_index(name='counts')

	return column_groupings



def log(columns, column_regex):
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
		filenames = get_filename_from_regex(file_regex)
		df = read_files(filenames)

		columns = get_column_regex_matches(df, column_regex)
		column_groupings = get_column_group_counts(df, columns)
		log(columns, column_regex, column_groupings)


		import ipdb; ipdb.set_trace()
		if value_regex:
			column_dict = get_column_value_regex_matches(df, columns, value_regex)
			# LOG
	
	finally:
		spark.session.stop()


if __name__ == "__main__":
	main()

# python -m buildamap.clients.common.files.search
