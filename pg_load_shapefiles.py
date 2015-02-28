import re
import os
import sys
import osgeo
import psycopg2
import argparse
from osgeo import ogr

# Set postgres parameters
pg_host = 'localhost'
pg_user = 'postgres'
pg_dbname = 'census'
pg_schema = 'tiger'

proj_dir = 'G:/PUBLIC/GIS_Projects/Census_Analysis/Census-Postgres/census-shapefile-utils'
data_dir = os.path.join(proj_dir, 'extracted_files')

geo_type_dict = {
	'tabblock10': 'block10',
	'bg': 'block_group',
	'tract': 'tract'
}

fld_type_dict = {
	'String': 'text',
	'Real': 'numeric'
}


def loadGeoData():
	""""""

	data_groups = defineDataGroups()
	pg_cur = createPgCursor()
	createTigerSchema()

	for tbl_name, shp_paths in data_groups.iteritems():
		shp_template = shp_paths[0]
		table_cmd = generateTableCommand(shp_template, tbl_name)
		print table_cmd


	#for feature in layer:

	pg_cur.close()

def defineDataGroups():
	""""""

	data_groups = {}
	geo_keys = [
		'tiger_line_label',
		'year',
		'state_code',
		'geo_type'
	]

	for sub_dir in os.listdir(data_dir):
		if re.match('\w+_\w+_\w+_\w+', sub_dir):
			geo_desc = dict(zip(geo_keys, sub_dir.split('_')))
			shp_dir = os.path.join(data_dir, sub_dir)
			
			for shp_part in os.listdir(shp_dir):
				if re.match('\w+\.shp$', shp_part):
					geo_type = geo_type_dict[geo_desc['geo_type']]
					geo_class = '{0}_{1}'.format(geo_type, geo_desc['year'])
					shp_path = os.path.join(shp_dir, shp_part)
					
					if geo_class not in data_groups:
						data_groups[geo_class] = [shp_path]
					else:
						data_groups[geo_class].append(shp_path)

	return data_groups

def createTigerSchema():
	""""""

	pg_cur = createPgCursor()
	schema_cmd="CREATE SCHEMA $pg_schema"
	pg_cur.extracted_files(schema_cmd)

	pg_cur.close()

def createPgCursor():
	""""""

	pg_template = 'dbname={0} user={1} host={2} password={3}'
	pg_str = pg_template.format(pg_dbase, pg_user, pg_host, pg_pass)
	pg_conn = psycopg2.connect(pg_str)
	pg_cur = pg_conn.cursor()

	return pg_cur

def generateTableCommand(shp_path, tbl_name):
	""""""

	field_list = []
	shp = osgeo.ogr.Open(shp_path)
	layer = shp.GetLayer(0)
	lyr_info = layer.GetLayerDefn()
	
	for i in range(lyr_info.GetFieldCount()):
		f_name = lyr_info.GetFieldDefn(i).GetName()
		f_type_id = lyr_info.GetFieldDefn(i).GetType()
		f_type = lyr_info.GetFieldDefn(i).GetFieldTypeName(f_type_id)

		field_list.append((f_name.lower(), fld_type_dict[f_type]))

	tbl_template =  """CREATE TABLE {0}.{1} (
						id serial,
						geom geometry,
						{1}
					)"""
	field_syntax = ', '.join([n + ' ' + t for n, t in field_list])
	table_cmd = tbl_template.format(pg_schema, tbl_name, field_syntax)

	return table_cmd

def process_options(arglist=None):
	""""""

	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-H', '--host',
		default='localhost',
		dest='pg_host',
		help='host for target postgresql database'
	)
	parser.add_argument(
		'-u', '--user', '--username',
		dest='pg_user',
		required=True,
		help='user name (role) for target postgresql database'
	)
	parser.add_argument(
		'-p', '--pass', '--password',
		dest='pg_password',
		required=True,
		help='password for target postgresql database'
	)
	parser.add_argument(
		'-d', '--dbname', '--database',
		default='census',
		dest='pg_dbname',
		help='name of target postgresql database'
	)
	parser.add_argument(
		'-s', '--schema',
		default='tiger',
		dest='pg_schema',
		help='target schema for data load into postgresql database'
	)

	options = parser.parse_args(arglist)
	return options

def main():
	"""
	>> python pg_load_shapefiles.py -h localhost -u postgres -d census
	"""

	global pg_host
	global pg_user
	global pg_password
	global pg_dbname
	global pg_schema

	args = sys.argv[1]
	options = process_options(args)

	pg_host = options.pg_host
	pg_user	= options.pg_user
	pg_password = options.pg_password
	pg_dbname = options.pg_dbname
	pg_password = options.pg_password

	loadGeoData()

if __name__ == '__main__':
	main()