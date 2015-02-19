census-shapefile-utils
======================

Tools for fetching shapefiles from the Census FTP site, then extracting
data from them.

### fetch_shapefiles.py ###

This script will download TIGER data shapefiles from the Census FTP site.
It can be used to download a set of geographies defined in `GEO_TYPES_LIST`,
or can be used to fetch files for specific states and/or geography types.
The default year for the data is 2014, but alternate years, dating back
to 2010, can be specific with the -y flag.  In contrast to the state and
geography type parameters only one data year can be indicated each time
the script is run.

    >> python fetch_shapefiles.py
    >> python fetch_shapefiles.py -s WA
    >> python fetch_shapefiles.py -s OR -g place -y 2012
    >> python fetch_shapefiles.py -s OR WA -g tract bg tabblock

If you use the -s argument to fetch files for specific states, the script
will also download the national county, state and congressional district
files that include data for your chosen states.

This will create `DOWNLOAD_DIR` and `EXTRACT_DIR` if necessary, fetch a zipfile
or set of zipfiles from the Census website, then extract the shapefiles from
each zipfile retrieved.

`DISABLE_AUTO_DOWNLOADS` will prevent certain geography types from being
automatically downloaded if no -g argument is passed to `fetch_shapefiles.py`.
This may be useful because certain files, such as those for Zip Code
Tabulation Areas, Census Tabulation Blocks, etc. which are extremely large. You
can still target any geography in `GEO_TYPES_LIST` specifically, however. So 
to fetch the ZCTA data execute the first command and for Tracts and Block Groups
use the second:

    >> python fetch_shapefiles.py -g zcta5
    >> python fetch_shapefiles.py -g tract bg


### parse_shapefiles.py ###

After you run `fetch_shapefiles.py`, this script will generate a csv file
from the extracted data. These files will have a normalized set of headers,
so varying geography types can be included in the same csv. Each row also gets
some useful additional fields not directly found in the shapefiles, Including:

* `FULL_GEOID`: Concatenated Census summary level code and Census GEOID
* `FULL_NAME`: Human-friendly name for the geography. So city names,
for instance, also include the state name, e.g. "Spokane, Washington"
* `SUMLEV`: Census summary level code
* `GEO_TYPE`: Name of the geography type, e.g. "state"
* `REGION`: Where applicable, a Census Region code. Shapefiles for states
include this code; this script infers the value based on state for other
geography types.
* `REGION_NAME`: Name of the Census Region, e.g. "West"
* `DIVISION`: Where applicable, a Census Division code. Shapefiles for states
include this code; this script infers the value based on state for other
geography types.
* `DIVISION_NAME`: Name of the Census Division, e.g. "Pacific"

This script will search all directories inside `EXTRACT_DIR` for shapefiles.
Pass an -s argument to limit by state, and/or pass a -g argument to limit
to a single geography type.

    >> python parse_shapefiles.py
    >> python parse_shapefiles.py -s WA
    >> python parse_shapefiles.py -g place
    >> python parse_shapefiles.py -s WA -g place
    
This script will generate a single csv file with your chosen data, and write
it to `CSV_DIR`. Headers are pulled from `helpers/csv_helpers.py`. The methods
for building rows specific to each geography type are also in `csv_helpers`.

You can choose whether the generated csv should include polygon geometries,
which can significantly increase the size of the output file. Include
geometries by passing a -p flag.

    >> python parse_shapefiles.py -s WA -p

Geometry data for certain geography types can be *very* large. The `zcta5`
geometries, for instance, will add about 1.1 Gb of data to your csv.

### Examples ###

These assume you have already used `fetch_shapefiles.py` to download
the shapefiles you want to get data from.

`>> python parse_shapefiles.py -g place` will make `place.csv`, which includes
data from all records at the Census place level.

`>> python parse_shapefiles.py -s WA` will make `all_geographies_WA.csv`,
which includes all geographies in Washington state, from the state record
all the way down to cities (places) and school districts. It will not include
polygon geometries.

`>> python parse_shapefiles.py -s WA -g county -p` will make `county_WA.csv`,
which includes data from all counties in Washington state. Because the -p flag
was passed, it will also include polygon geometries for each record.

`>> python parse_shapefiles.py` will make `all_geographies.csv`, which
includes data from all geography levels and all states. If you've downloaded
shapefiles for all levels, including for Zip Code Tabulation Areas, the csv
file should be about 19 Mb.

`>> python parse_shapefiles.py -p` will make the same file as above, but
including geometries. This file takes about 17 minutes to build locally on my
Macbook Air, and is about 2.45 Gb.
