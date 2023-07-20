import os
import unittest
import search

import pandas as pd


class TestSearchFunctionality(unittest.TestCase):


	def setUp(self):
		# generate a csv with 4 columns and 4 rows with randomized words containing at least 4 characters in each row
		self.dataframe = pd.DataFrame(
			{
				'column1': ['abcd', 'efgh', 'ijkl', 'mnop'],
				'column2': ['qrst', 'uvwx', 'yzab', 'cdef'],
				'column3': ['ghij', 'klmn', 'opqr', 'stuv'],
				'column4': ['wxyz', 'abcd', 'efgh', 'ijkl']
			}
		)


		# save the dataframe to a csv
		self.dataframe.to_csv('data1.csv', index=False)

		# shuffle the dataframe and save to a new csv, do this twice
		self.dataframe = self.dataframe.sample(frac=1).reset_index(drop=True)

		self.dataframe.to_csv('data2.csv', index=False)

		self.dataframe = self.dataframe.sample(frac=1).reset_index(drop=True)

		self.dataframe.to_csv('data3.csv', index=False)


	def tearDown(self):
		# delete the csv files
		os.remove('data1.csv')
		os.remove('data2.csv')
		os.remove('data3.csv')


	# create a test that checks that the function get_filename_from_regex returns the correct files
	def test_get_filename_from_regex(self):
		# create a list of the filenames
		filenames = ['data1.csv', 'data2.csv', 'data3.csv']

		# create a regex to match the filenames
		filename_regex = 'data[0-9].csv'

		# call the function
		filename_matches = search.get_filename_from_regex(filename_regex)

		# sort filename_matches by descending order
		filename_matches.sort(reverse=False)

		# check that the returned list is the same as the list of filenames
		self.assertEqual(filename_matches, filenames)


	# create a test that checks that the function infer_separator returns the correct separator
	def test_infer_separator(self):

		# create a regex to match the filenames
		filename_regex = 'data[0-9].csv'

		# call the function
		filename_matches = search.get_filename_from_regex(filename_regex)

		# call the function
		separator = search.infer_separator(filename_matches[0])

		# check that the returned separator is a comma
		self.assertEqual(separator, ',')


	# create a test that checks that the function read_files returns a non-empty dataframe
	def test_read_files(self):

		# create a regex to match the filenames
		filename_regex = 'data[0-9].csv'

		# call the function
		filename_matches = search.get_filename_from_regex(filename_regex)

		# call the function
		separator = search.infer_separator(filename_matches[0])

		# call the function
		dataframe = search.read_files(filename_matches, separator)

		# check that the dataframe is not empty
		self.assertFalse(dataframe.empty)


	# create a test that checks that the function get_column_regex_matches returns the correct columns
	def test_get_column_regex_matches(self):

		# create a regex to match the filenames
		filename_regex = 'data[0-9].csv'

		# call the function
		filename_matches = search.get_filename_from_regex(filename_regex)

		# call the function
		separator = search.infer_separator(filename_matches[0])

		# call the function
		dataframe = search.read_files(filename_matches, separator)

		# create a regex to match the columns
		column_regex = 'column'

		# call the function
		column_matches = search.get_column_regex_matches(dataframe, column_regex)

		# check that the returned list is the same as the list of columns
		self.assertEqual(column_matches, ['column1', 'column2', 'column3', 'column4'])


	# create a test that checks that the function get_column_group_counts returns the a non-empty dictionary
	def test_get_column_group_counts(self):

		# create a regex to match the filenames
		filename_regex = 'data[0-9].csv'

		# call the function
		filename_matches = search.get_filename_from_regex(filename_regex)

		# call the function
		separator = search.infer_separator(filename_matches[0])

		# call the function
		dataframe = search.read_files(filename_matches, separator)

		# create a regex to match the columns
		column_regex = 'column'

		# call the function
		column_matches = search.get_column_regex_matches(dataframe, column_regex)

		# call the function
		column_group_counts = search.get_column_group_counts(dataframe, column_matches)

		# check that the returned dictionary is not empty
		self.assertTrue(column_group_counts)


	# create a test that checks that the function filter_null_values does not return any null values
	def test_filter_null_values(self):

		# create a regex to match the filenames
		filename_regex = 'data[0-9].csv'

		# call the function
		filename_matches = search.get_filename_from_regex(filename_regex)

		# call the function
		separator = search.infer_separator(filename_matches[0])

		# call the function
		dataframe = search.read_files(filename_matches, separator)

		# create a regex to match the columns
		column_regex = 'column'

		# call the function
		column_matches = search.get_column_regex_matches(dataframe, column_regex)

		# call the function
		column_group_counts = search.get_column_group_counts(dataframe, column_matches)

		# call the function
		filtered_dataframe = search.filter_null_values(dataframe, column_group_counts)

		# check that the returned dataframe has no null values
		self.assertFalse(filtered_dataframe.isnull().values.any())


	# create a test that checks that the function filter_for_regex_pattern filters the dataframe correctly
	def test_filter_for_regex_pattern(self):

		# create a regex to match the filenames
		filename_regex = 'data[0-9].csv'

		# call the function
		filename_matches = search.get_filename_from_regex(filename_regex)

		# call the function
		separator = search.infer_separator(filename_matches[0])

		# call the function
		dataframe = search.read_files(filename_matches, separator)

		# call the function
		column_group_counts = search.get_column_group_counts(dataframe, ['column1'])

		# call the function
		filtered_dataframe = search.filter_null_values(dataframe, column_group_counts)

		# create a regex to match the pattern
		pattern = 'mnop'

		# call the function
		filtered_dataframe = search.filter_for_regex_pattern(filtered_dataframe, column_group_counts, pattern)

		# check that the returned dataframe is not empty
		self.assertFalse(filtered_dataframe.empty)
		
	# create a test that checks that the function get_column_value_regex_matches returns the correct regex matches
	def test_get_column_value_regex_matches(self):
		
		# call the function
		dataframe = search.read_files(['data1.csv', 'data2.csv', 'data3.csv'], ',')

		# create a regex to match the columns
		column_regex = 'column'

		# call the function
		column_matches = search.get_column_regex_matches(dataframe, column_regex)

		# call the function
		column_group_counts = search.get_column_group_counts(dataframe, column_matches)

		# call the function
		filtered_dataframe = search.filter_null_values(dataframe, column_group_counts)

		# create a regex to match the pattern
		pattern = 'mnop'

		# call the function
		filtered_dataframe = search.filter_for_regex_pattern(filtered_dataframe, column_group_counts, pattern)

		# call the function
		column_value_regex_matches = search.get_column_value_regex_matches(filtered_dataframe, column_group_counts, pattern)

		# check that the returned list is non empty
		self.assertTrue(column_value_regex_matches)

if __name__ == "__main__":
	unittest.main()
