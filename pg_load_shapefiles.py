import re
import os
import sys
import osgeo
import psycopg2
import argparse
from osgeo import ogr

proj_dir = 'G:/PUBLIC/GIS_Projects/Census_Analysis/Census-Postgres/census-shapefile-utils'
data_dir = os.path.join(proj_dir, 'extracted_files')
tiger_epsg = 4269
or_spn_epsg = 2913

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
	"""Load tiger shapefiles into a postgres database, grouping those that share
	the same geo-type and data year into common tables"""

	conn, cur = createPgCursor()
	data_groups = defineDataGroups()

	# create schema to hold tiger data if it doesn't already exist,
	# if it does the existing schema will remain and the error is handled
	schema_cmd = "CREATE SCHEMA {0};".format(pg_schema)
	try:
		cur.execute(schema_cmd)
		print schema_cmd
		conn.commit()
	except psycopg2.ProgrammingError as e:
		conn.rollback()
		print e.message
		print 'skipping schema creation'

	for tbl_name, shp_paths in data_groups.iteritems():
		for i, path in enumerate(shp_paths):
			shp = osgeo.ogr.Open(path)
			layer = shp.GetLayer(0)

			if i == 0:
				table_specs = generateTableCommands(layer, tbl_name)
				drop_tbl = table_specs['dt']
				create_tbl = table_specs['ct']
				field_info = table_specs['fi']
				
				# drop the table if it exists then recreate it
				cur.execute(drop_tbl)
				cur.execute(create_tbl)
				conn.commit()

			fc_name = os.path.basename(path)
			print 'Loading feature class: {0}'.format(fc_name)
			
			for j, feature in enumerate(layer, 1):
				insert_cmd = generateInsertCommand(
					feature, field_info, tbl_name, cur)
				cur.execute(insert_cmd)

				if j % 500 == 0:
					sys.stdout.write('X')
			
			conn.commit()
			sys.stdout.write('\n\n')

		# Add a primary key and spatial index to the newly
		# populated table
		ix_cmds = generateIxCommands(tbl_name)
		pk_cmd = ix_cmds['pk_cmd']
		geom_ix_cmd = ix_cmds['geom_ix_cmd']

		ix_msg = 'creating primary key and spatial index for {0}'
		print ix_msg.format(tbl_name)
		cur.execute(pk_cmd)
		cur.execute(geom_ix_cmd)
		conn.commit()

	cur.close()
	conn.close()

def defineDataGroups():
	""""Group datasets that are of the same year and geo-type so that each group
	cab be loaded into a single table"""

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

def createPgCursor():
	"""Create a database cursor that can run queries against the db defined
	at the top of this script"""

	conn_template = 'dbname={0} user={1} host={2} password={3}'
	conn_str = conn_template.format(pg_dbname, pg_user, pg_host, pg_password)
	conn = psycopg2.connect(conn_str)
	cur = conn.cursor()

	return (conn, cur)

def generateTableCommands(layer, tbl_name):
	"""Create a SQL command that will generate a table with a schema that
	matches that of the shapefile that is passed to it"""

	field_info = []
	lyr_info = layer.GetLayerDefn()
	
	for i in range(lyr_info.GetFieldCount()):
		f_name = lyr_info.GetFieldDefn(i).GetName()
		f_type_id = lyr_info.GetFieldDefn(i).GetType()
		f_type = lyr_info.GetFieldDefn(i).GetFieldTypeName(f_type_id)
		field_info.append((f_name, fld_type_dict[f_type]))

	tbl_template =  """CREATE TABLE {0}.{1} (
						geom GEOMETRY,
						{2}
					);"""
	field_syntax = ', '.join([n.lower() + ' ' + t for n, t in field_info])
	create_tbl = tbl_template.format(pg_schema, tbl_name, field_syntax)
	drop_tbl = "DROP TABLE IF EXISTS {0}.{1} CASCADE;".format(pg_schema, tbl_name)
	
	return {
		'ct': create_tbl,
		'dt': drop_tbl,
		'fi': field_info
	}

def generateInsertCommand(feature, field_info, tbl_name, cur):
	"""Based on a feature (row) from a shapefile generate a command
	that will insert that information into a postgresql table"""

	# in addition to inserting the data this command will reproject it
	# to the epsg provided in parameter 2
	insert_template = """INSERT INTO {0}.{1}
						VALUES (ST_Transform(ST_GeomFromText({2}, {3}), {4}),
						{5}
						);"""
	wkt_geom = '\'{0}\''.format(feature.GetGeometryRef().ExportToWkt())
	
	# mogrify maps values into a string that will preserve their types for
	# a sql query (for instance strings will be quotes and numbers will not)
	fv_list = [feature.GetField(fn) for fn, ft in field_info]
	mog_str = ', '.join(['%s' for fv in fv_list])
	fv_syntax = cur.mogrify(mog_str, fv_list)
	
	insert_cmd = insert_template.format(
		pg_schema,
		tbl_name, 
		wkt_geom, 
		tiger_epsg, 
		or_spn_epsg, 
		fv_syntax
	)
	return insert_cmd

def generateIxCommands(tbl_name):
	""""""

	pk_template = """ALTER TABLE {0}.{1} ADD id SERIAL PRIMARY KEY;"""
	pk_cmd = pk_template.format(pg_schema, tbl_name)

	geom_ix_template = """CREATE INDEX {0} ON {1}.{2} USING GIST (geom);"""
	geom_ix_name = '{0}_geom_ix'.format(tbl_name)
	geom_ix_cmd = geom_ix_template.format(geom_ix_name, pg_schema, tbl_name)

	return {
		'pk_cmd': pk_cmd, 
		'geom_ix_cmd': geom_ix_cmd
	}

def process_options(arglist=None):
	"""Define options that users can pass through the command line, in this
	case these are all postgres database parameters"""

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
	>> python pg_load_shapefiles.py -H localhost -u postgres -d census
	"""

	global pg_host
	global pg_user
	global pg_password
	global pg_dbname
	global pg_schema

	args = sys.argv[1:]
	options = process_options(args)

	pg_host = options.pg_host
	pg_user	= options.pg_user
	pg_password = options.pg_password
	pg_dbname = options.pg_dbname
	pg_schema = options.pg_schema

	loadGeoData()

if __name__ == '__main__':
	main()