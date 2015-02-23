import re
import os
import psycopg2
import osgeo

proj_dir = '/Users/granthumphries/Coding/git/census-shapefile-utils/extracted_files'
pg_str = 'dbname=census user=granthumphries'
#conn = psycopg2.connect()


#for shp_name in os.listdir()
#	if re.match('w/+.shp$', shp_path):

shp_path = os.path.join(proj_dir, 'tl_2014_41_tabblock10/tl_2014_41_tabblock10.shp')
shp = osgeo.ogr.Open(shp_path)
layer = shp.GetLayer(0)
#for feature in layer:

geo_type_dict = {
	'tabblock': 'block',
	'bg': 'block_group',
	'tract': 'tract'
}

fld_type_dict = {
	'String': 'text',
	'Real': 'numeric'
}

def generateTableSchema(layer):
	""""""

	field_list = []
	lyr_info = layer.GetLayerDefn()
	for i in range(lyr_info.GetFieldCount()):
		f_name = lyr_info.GetFieldDefn(i).GetName()
		f_type_id = lyr_info.GetFieldDefn(i).GetType()
		f_type = lyr_info.GetFieldDefn(i).GetFieldTypeName(f_type_id)

		field_list.append((f_name.lower(), fld_type_dict[ftype]))

	tbl_template =  """CREATE TABLE {0} (
						id serial
						geom geometry,
						{1}
					)"""
	field_syntax = ', '.join([n + ' ' + t for n, t in field_list])
	table_cmd = tbl_template.format(, field_syntax)
	print field_syntax

generateTableSchema(layer)