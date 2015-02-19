'''
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
'''

import sys, argparse, os, traceback, urllib2, zipfile
from os.path import isdir, join, normpath, split
from datetime import date

from __init__ import (DOWNLOAD_DIR, EXTRACT_DIR, STATE_ABBREV_LIST, \
    GEO_TYPES_LIST, DISABLE_AUTO_DOWNLOADS, get_fips_code_for_state)

FTP_HOME = 'ftp://ftp2.census.gov/geo/tiger/TIGER{0}/'


def get_filename_list_from_ftp(target, state):
    target_files = urllib2.urlopen(target).read().splitlines()
    filename_list = []

    for line in target_files:
        filename = '%s%s' % (target, line.split()[-1])
        filename_list.append(filename)

    if state:
        state_check = '_%s_' % get_fips_code_for_state(state)
        filename_list = filter(
            lambda filename: state_check in filename \
            or ('_us_' in filename and '_us_zcta5' not in filename),
            filename_list
        )

    return filename_list


def download_files_in_list(filename_list):
    downloaded_filename_list = []
    for file_location in filename_list:
        filename = '%s/%s' % (DOWNLOAD_DIR, file_location.split('/')[-1])
        u = urllib2.urlopen(file_location)
        f = open(filename, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])

        print "Downloading: %s Bytes: %s" % (filename, file_size)
        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = status + chr(8)*(len(status)+1)
            print status,

        f.close()
        downloaded_filename_list.append(filename)
    
    return downloaded_filename_list
    

def extract_downloaded_file(filename):
    zipped = zipfile.ZipFile(filename, 'r')
    zip_dir = filename.replace('.zip','').split('/')[-1]
    target_dir = normpath(join(EXTRACT_DIR, zip_dir))

    print "Extracting: " + filename + " ..."
    zipped.extractall(target_dir)
    zipped.close()


def get_one_geo_type(geo_type, state=None):
    target = '%s%s/' % (FTP_HOME, geo_type.upper())

    print "Finding files in: " + target + " ..."
    filename_list = get_filename_list_from_ftp(target, state)
    downloaded_filename_list = download_files_in_list(filename_list)

    for filename in downloaded_filename_list:
        extract_downloaded_file(filename)


def get_all_geo_types(state=None):
    AUTO_DOWNLOADS = filter(
        lambda geo_type: geo_type not in DISABLE_AUTO_DOWNLOADS,
        GEO_TYPES_LIST
    )
    for geo_type in AUTO_DOWNLOADS:
        get_one_geo_type(geo_type, state)


def process_options(arglist=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--state',
        nargs='+',
        dest='state',
        help='specific state(s) to download',
        choices=STATE_ABBREV_LIST
    )
    parser.add_argument(
        '-g', '--geo', '--geo_type',
        nargs='+',
        dest='geo_type',
        help='specific geographic type(s) to download',
        choices=GEO_TYPES_LIST
    )
    parser.add_argument(
        '-y', '--year', '--tiger_year',
        default=2014,
        type=int,
        dest='year',
        help='year to which the tiger data pertains',
        choices=range(2010, (date.today().year+1))
    )

    options = parser.parse_args(arglist)
    return options


def main(args=None):
    """
    >> python fetch_shapefiles.py
    >> python fetch_shapefiles.py -s WA
    >> python fetch_shapefiles.py -g place
    >> python fetch_shapefiles.py -s WA -g place
    >> python fetch_shapefiles.py -s OR WA -g tract bg -y 2014
    """
    
    global FTP_HOME
    
    # make sure we have the expected directories
    for path in [DOWNLOAD_DIR, EXTRACT_DIR]:
        if not isdir(path):
            os.mkdir(path)

    if args is None:
        args = sys.argv[1:]
    options = process_options(args)

    # Assign parameter data to variables
    geo_types = options.geo_type
    states = options.state or [None]
    FTP_HOME = FTP_HOME.format(options.year)

    # get data for geotypes and states passed to script (if parameter(s)
    # aren't provided data for all values in the class(es))
    if geo_types:
        for gt in geo_types:
            for s in states:
                get_one_geo_type(
                    geo_type=gt,
                    state=s
                )
    else:
        for s in states:
            get_all_geo_types(
                state=s
            )


if __name__ == '__main__':
    try:
        main()
    except Exception, e:
        sys.stderr.write('\n')
        traceback.print_exc(file=sys.stderr)
        sys.stderr.write('\n')
        sys.exit(1)

