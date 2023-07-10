import click

from pyspark.sql import functions as F, Window, types as spark_types

from buildamap import filesystem as fs
from buildamap.utils import spark
from buildamap.common import getlogger
from buildamap.rds.utils.postgres_requests import list_audiences
from buildamap.parse_bam_config import OverriddenConfig, get_bam_config



logger = getlogger(__name__)
#BUCKETS = ['customers', 'raws', 'uploads']
#SEPARATORS = [',', '|', '\t']
SEPARATORS = ['|', '\t']
BUCKET_LOG = '{} bucket:'
FILE_SEARCH_LOG = 'Seaching for files containing "{}".'
NO_FILE_EXCEPTION = 'No files names containing "{}" keyword.'
COLUMN_MATCH_LOG = 'Column Regex Match:'


def get_filename_from_regex(file_regex, fs_api):
	try:
		return fs_api.ls(regex=file_regex)
	except IOError as e:
		raise fs.utils.FileNotFoundError(' '.join([
			BUCKET_LOG.format(fs_api.bucket_name),
			NO_FILE_EXCEPTION.format(file_regex)
			]))



def infer_separator(file, fs_api):
	import ipdb; ipdb.set_trace()
	file_object = fs_api.read.from_csv(file)
	
	for separator in SEPARATORS:
		df = file_object.to_pandas(sep=separator, nrows=1)
		if len(df.columns) > 1:
			return separator

	raise Exception('Cannot Infer File Separator')


def read_files(filenames, fs_api):
	separator = infer_separator(filenames[0], fs_api)

	data = fs_api.read.from_csv(filenames).to_spark(sep=separator)

	return data


def _file_context_manager(func, file_details, fs_api, gcs):
	if gcs:
		with OverriddenConfig({"StorageBackend": "GCS"}):
			return func(file_details, fs_api)
	else:
		return func(file_details, fs_api)


def get_column_regex_matches(df, column_regex):
	return [c for c in df.columns if column_regex in c]


def log_column_matches(columns, bucket, column_regex):
	if columns:
		logger.info(
			'\n'.join(
			COLUMN_MATCH_LOG,
			BUCKET_LOG.format(bucket),
			'\n\t'.join(columns)
		))
	else:
		logger.info(
			"Cannot locate {} keyword in files.".format(column_regex)
		)


def concatenate_filters(columns):
	return ' OR '.join(
		["{} IS NOT NULL".format(col) for col in columns]
	)


def get_column_value_regex_matches(df, columns, value_regex):
	filters = concatenate_filters(columns)
	df_filtered = df.where(F.expr(filters)).persist()

	column_dict = {column: df_filtered.filter(F.col(column).contains(value_regex)).count() for column in columns}
	
	return column_dict

@click.command()
@click.option("--client_id", "-c", required=True)
@click.option("--environment", "-n", default=None)
@click.option("--gcs", is_flag=True, default=None)
@click.option("--file_regex", "-file", required=True, default=None)
@click.option("--column_regex", "-col", required=True, default=None)
@click.option("--value_regex", "-val", default=None)
@click.option("--bucket", "-b", default=None, type=str)
def main(client_id, environment, file_regex, gcs, column_regex, value_regex, bucket):
	config = get_bam_config(client_id=client_id, environment=environment)
	spark.session.get_or_create(max_cores=64)
	import ipdb; ipdb.set_trace()
	try:

		fs_api = getattr(fs, bucket)
		filenames = _file_context_manager(get_filename_from_regex, file_regex, fs_api, gcs)
		df = _file_context_manager(read_files, filenames, fs_api, gcs)

		columns = get_column_regex_matches(df, column_regex)
		log_column_matches(columns, bucket, column_regex)

		import ipdb; ipdb.set_trace()
		if column_regex and value_regex:
			column_dict = get_column_value_regex_matches(df, columns, value_regex)
			# LOG
	
	finally:
		spark.session.stop()


if __name__ == "__main__":
	main()

# python -m buildamap.clients.common.files.search
