#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
/***************************************************************************
Name                 : Daily precipitation GPM
Description          : Create a CSV file with values of daily precipitation
                       from FTP (arthurhou.pps.eosdis.nasa.gov)
                       - CSV Fields:  ID station | date | total_mm(APD)
                       - APD (Accumulation Precipitation of Day):
                         Previuos day(12:00 to 23:59) + Current day(00:00 to 11:59)
Date                 : March, 2020
copyright            : (C) 2020 by Luiz Motta
email                : motta.luiz@gmail.com

 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""
__author__ = 'Luiz Motta'
__date__ = '2020-03-16'
__copyright__ = '(C) 2020, Luiz Motta'
__revision__ = '$Format:%H$'
 
import sys, os, csv
from datetime import datetime, timedelta
import urllib.request, urllib.error
import argparse, re, struct
from multiprocessing.pool import ThreadPool

from osgeo import gdal
from osgeo.gdalconst import GA_ReadOnly

gdal.UseExceptions()

class DatasetValuePixel():
    FMTTYPES = {
        'Byte':'B',
        'UInt16':'H',
        'Int16':'h',
        'UInt32':'I',
        'Int32':'i',
        'Float32':'f',
        'Float64':'d'
    }

    def __init__(self, dataset):
        self.ds = dataset
        transf = dataset.GetGeoTransform()
        self.transfInv = gdal.InvGeoTransform( transf )
        self.band,self.fmt  = None, None

    def setBand(self, numberBand):
        self.band = self.ds.GetRasterBand( numberBand )
        nameType = gdal.GetDataTypeName( self.band.DataType )
        self.fmt = self.FMTTYPES[ nameType ]

    def getValue(self, x, y):
        px, py = gdal.ApplyGeoTransform( self.transfInv, x, y )
        structval = self.band.ReadRaster( int(px), int(py), 1, 1, buf_type=self.band.DataType )
        val = struct.unpack( self.fmt , structval)
        return round( val[0], 2 )


class GpmDataset():
    HOST = 'arthurhou.pps.eosdis.nasa.gov'
    TYPE_IMAGE = '3B-HHR-GIS.MS.MRG.3IMERG'
    VERSION = 6
    CONFIG_TIME = {
        'iniHourBefore': 12,
        'deltaStep': timedelta(minutes=30),
        'deltaSecond': timedelta(seconds=1)
    }
    IMAGES_DAY = 48 # Day(24h) = 48 * 1/2 hour
    VSICURL = False
    @staticmethod
    def getValuesDatatime(v_datetime):
        """
        Args:
            v_datetime: datetime
        Return: Generator of dictionary of valueDatetime(see getNameImage)
                Previuos day(12:00 to 23:59) and Current day(00:00 to 11:59)
        """
        previosDay = v_datetime - timedelta(days=1)
        args = ( previosDay.year, previosDay.month, previosDay.day, GpmDataset.CONFIG_TIME['iniHourBefore'] )
        s_dt = datetime( *args )
        for _i in range( GpmDataset.IMAGES_DAY ):
            e_dt = s_dt + GpmDataset.CONFIG_TIME['deltaStep'] - GpmDataset.CONFIG_TIME['deltaSecond']
            v = {
                'year': s_dt.year,
                'month': s_dt.month,
                'day': s_dt.day,
                's_hour': s_dt.hour,
                's_minute': s_dt.minute,
                's_second': s_dt.second,
                'e_hour': e_dt.hour,
                'e_minute': e_dt.minute,
                'e_second': e_dt.second,
                'totalmin': s_dt.hour * 60  + s_dt.minute
            }
            s_dt += GpmDataset.CONFIG_TIME['deltaStep']
            yield v

    @staticmethod
    def getNameImage(valueDatetime):
        """
        Args:
            valueDatetime:
                {
                    'year', 'month', 'day',
                    's_hour', 's_minute', 's_second',
                    'e_hour', 'e_minute', 'e_second',
                    'totalmin'
                }
        """
        f_image = {
            'type': GpmDataset.TYPE_IMAGE,
            'day': '{year:04}{month:02}{day:02}',
            'start': 'S{s_hour:02}{s_minute:02}{s_second:02}',
            'end': 'E{e_hour:02}{e_minute:02}{e_second:02}',
            'totalmin': '{totalmin:04}',
            'version': f"V{GpmDataset.VERSION:02}B"
        }
        f_name = "{type}.{day}-{start}-{end}.{totalmin}.{version}".format( **f_image )
        return f_name.format( **valueDatetime )

    def __init__(self, email, dirname):
        """
        Example image: 3B-HHR-GIS.MS.MRG.3IMERG.20170227-S013000-E015959.0090.V06B.tif
        - Type: 3B-HHR-GIS.MS.MRG.3IMERG
        - Day: .20170227 (YYYYmmDD)
        - Start: S013000 (HHMMSS)
        - End:   E015959 (HHMMSS)
        - Total minutes of day: 0090 (DDDD)
        - Version: V06B (version = 6)
        """
        user = email.replace('@', '%40')
        f_image = {
            'root': f"ftp://{user}:{user}@{self.HOST}/gpmdata",
            'dir': '{year:04}/{month:02}/{day:02}/gis',
            'type': self.TYPE_IMAGE,
            'day': '{year:04}{month:02}{day:02}',
            'start': 'S{s_hour:02}{s_minute:02}{s_second:02}',
            'end': 'E{e_hour:02}{e_minute:02}{e_second:02}',
            'totalmin': '{totalmin:04}',
            'version': f"V{self.VERSION:02}B"
        }
        self.ftp_image = "{root}/{dir}/{type}.{day}-{start}-{end}.{totalmin}.{version}.tif".format( **f_image )
        self.dirname = dirname
        self.getDS = self._getDS_Vsicurl if self.VSICURL else self._getDS_Download
        
    def isLive(self, v_datetime):
        """
        Args:
            v_datetime: datetime
        """
        valueDatetime = {
            'year': v_datetime.year, 'month': v_datetime.month, 'day': v_datetime.day,
            'e_hour': 12, 'e_minute': 29, 'e_second': 59,
            's_hour': 12, 's_minute': 0,  's_second': 0,
            'totalmin': 720
        }
        url = self.ftp_image.format( **valueDatetime )
        try:
            _response = urllib.request.urlopen(url, timeout=5)
        except urllib.error.URLError as e:
            return { 'isOk': False, 'message': f"Host: '{self.HOST}'\nUrl: {url}\n{e.reason}" }
        
        return { 'isOk': True }

    def _getDS_Vsicurl(self, url):
        ds = None
        url = f"/vsicurl/{url}"
        try:
            ds = gdal.Open( url, GA_ReadOnly )
        except RuntimeError: # gdal
            msg = f"Url '{url}': Error open image"
            return { 'isOk': False, 'message': msg }
        return { 'isOk': True, 'dataset': ds }

    def _getDS_Download(self, url):
        ds = None
        image = os.path.join( self.dirname, url.split('/')[-1] )
        try:
            if not os.path.exists( image ):
                _response = urllib.request.urlopen(url, timeout=5) # Check if can access
                urllib.request.urlretrieve( url, image )
            ds = gdal.Open( image, GA_ReadOnly )
        except urllib.error.URLError as e: # urllib
            msg = f"Url '{url}': n{e.reason}"
            return { 'isOk': False, 'message': msg }
        except RuntimeError: # gdal
            os.remove( image )
            msg = f"Url '{url}': Error open image"
            return { 'isOk': False, 'message': msg }
        return { 'isOk': True, 'dataset': ds }

    def getDataSet(self, valueDatetime):
        """
        Args:
            valueDatetime:
                {
                    'year', 'month', 'day',
                    's_hour', 's_minute', 's_second',
                    'e_hour', 'e_minute', 'e_second',
                    'totalmin'
                }
        """
        url = self.ftp_image.format( **valueDatetime )
        return self.getDS( url )


class CalculateGpm():
    def __init__(self, email, dateIni, dateEnd, filePathCsv):
        self.gpmDS = GpmDataset( email,  os.path.dirname( filePathCsv ) )
        self.dateIni, self.dateEnd = dateIni, dateEnd
        self.filePathCsv = filePathCsv
        self.factor_mm_day = 20
        self.stations = [] # { 'id', 'lat', 'long' }

    def init(self):
        def getDate(sDate):
            vdate = None
            try:
                vdate = datetime.strptime( sDate, '%Y-%m-%d')
            except ValueError:
                msg = f"No valid date '{sDate}' (YYYY-MM-DD)"
                return { 'isOk': False, 'message': msg }
            except:
                msg = f"Unexpected error with date '{sDate}' (YYYY-MM-DD)"
                return { 'isOk': False, 'message': msg }
            return { 'isOk': True, 'date': vdate }

        def setStations():
            with open( self.filePathCsv) as csvfile:
                rows = csv.reader( csvfile, delimiter=';' )
                next( rows )
                for row in rows:
                    station = {
                        'id': row[0],
                        'lat':  float( row[1] ),
                        'long': float( row[2] ),
                    }
                    self.stations.append( station )

        r = getDate( self.dateIni )
        if not r['isOk']:
            return r
        self.dateIni = r['date']

        r = getDate( self.dateEnd )
        if not r['isOk']:
            return r
        self.dateEnd = r['date']

        if self.dateIni > self.dateEnd:
            lblIni = self.dateIni.strftime('%Y-%m-%d')
            lblEnd = self.dateEnd.strftime('%Y-%m-%d')
            msg = f"ini_date({lblIni}) > end_date({lblEnd})"
            return { 'isOk': False, 'message': msg }

        r = self.gpmDS.isLive( self.dateIni )
        if not r['isOk']:
            return r

        if not os.path.isfile( self.filePathCsv ):
            msg = f"Missing file '{self.filePathCsv}'"
            return { 'isOk': False, 'message': msg }
        setStations()

        return { 'isOk': True }

    def saveCsv(self, download_keep):
        def printStatus(message):
            msg = f"\r{message.ljust(100)}"
            sys.stdout.write( msg )
            sys.stdout.flush()

        def createWriteFile(filepath, head=None):
            csvfile = open( filepath, mode='w')
            writer = csv.writer( csvfile, delimiter=';' )
            if head: writer.writerow( head )
            return { 'csvfile': csvfile, 'writerows': writer.writerows }

        def getTotalPrecipitation(data):
            """
            data: { 'datetime', 'labelDate' }
            return: { 'stations_total', 'errors' }
            """
            def getDatasetSources():
                c_images = 0
                sources, errors = [], []
                for vd in GpmDataset.getValuesDatatime( data['datetime'] ):
                    c_images += 1
                    name = GpmDataset.getNameImage( vd )
                    msg = f"{data['labelDate']} - Fetching {name} ({c_images}/{GpmDataset.IMAGES_DAY})..."
                    printStatus( msg )
                    r = self.gpmDS.getDataSet( vd )
                    sources.append( r['dataset'].GetDescription() ) if r['isOk'] else errors.append( r['message'] )

                return { 'sources': sources, 'errors': errors }

            def getStationsPrecipitations(source):
                """
                Args:
                    source: Source of Dataset
                """
                ds = gdal.Open( source, GA_ReadOnly )
                dvp = DatasetValuePixel( ds )
                dvp.setBand(1)
                station_precipitation = [] # ( id, value)
                for station in self.stations:
                    item = ( station['id'], dvp.getValue( station['long'], station['lat'] ) )
                    station_precipitation.append( item )
                ds = None

                return station_precipitation
            
            r = getDatasetSources()
            sources = r['sources']
            errors = r['errors']

            msg = f"{data['labelDate']} - Precipitations calculating..."
            printStatus( msg )
            pool = ThreadPool(processes=4)
            mapResult = pool.map_async( getStationsPrecipitations, sources )
            stations_total = { k['id']: 0 for k in self.stations }
            for results in mapResult.get():
                for k,v in results: stations_total[ k ] += v
            pool.close()
            if not download_keep:
                for src in sources: os.remove( src )
            sources.clear()
            return { 'stations_total': stations_total, 'errors': errors }

        suffix = f"{self.dateIni.strftime('%Y-%m-%d')}_{self.dateEnd.strftime('%Y-%m-%d')}"
        name = f"{os.path.splitext( self.filePathCsv )[0]}_gpm_{suffix}"
        filePathOut = f"{name}.csv"
        fwOut = createWriteFile( filePathOut, ['id', 'date', 'total_mm'] )

        filePathError = f"{name}_error.csv"
        fwError = createWriteFile( filePathError, ['date', 'message'] )
        totalError = 0
        
        delta = self.dateEnd - self.dateIni
        totalDays = delta.days + 1
        msg = f"{totalDays} Days | {GpmDataset.IMAGES_DAY} Images/Day | {len( self.stations )} Stations"
        print( msg )
        c_days = 0
        try:
            for d in range( totalDays ):
                dt = ( self.dateIni + timedelta(days=d) )
                c_days += 1
                labelDate = dt.strftime('%Y-%m-%d')
                data = { 'datetime': dt, 'labelDate': f"{labelDate} ({c_days}/{totalDays})" }
                r  = getTotalPrecipitation( data )
                if r['errors']:
                    totalError += len( r['errors'] )
                    items = ( [ labelDate, error ] for error in r['errors'] )
                    fwError['writerows']( items )
                    fwError['csvfile'].flush()
                items = ( [ k, labelDate, v/self.factor_mm_day ] for k, v in r['stations_total'].items() )
                fwOut['writerows']( items )
                fwOut['csvfile'].flush()
        except Exception as e:
            print(f"\nError processing: {str(e)}\n")
        fwOut['csvfile'].close()
        fwError['csvfile'].close()
        msg = f"Saved '{filePathOut}'."
        printStatus( msg )
        if not totalError:
            os.remove( filePathError )
        else:
            msg = f"\nErrors read images ({totalError} images): '{filePathError}'\r"
            print( msg )


class EmailType(object):
    """
    Adaptation from https://gist.github.com/asfaltboy/79a02a2b9871501af5f00c95daaeb6e7
    Supports checking email agains different patterns. The current available patterns is:
    RFC5322 (http://www.ietf.org/rfc/rfc5322.txt)
    """
    patterns = {
        'RFC5322': re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"),
    }

    def __init__(self, pattern):
        if pattern not in self.patterns:
            msg = f"{pattern} is not a supported email pattern, choose from: {','.join(self.patterns)}"
            raise KeyError( msg )
        self._rules = pattern
        self._pattern = self.patterns[pattern]

    def __call__(self, value):
        if not self._pattern.match(value):
            msg = f"'{value}' is not a valid email - does not match {self._rules} rules"
            raise argparse.ArgumentTypeError( msg )
        return value


def run(email, ini_date, end_date, filepath_csv, download_keep):
    def messageDiffDateTime(dt1, dt2):
        diff = dt2 - dt1
        return "Days = {} hours = {}".format( diff.days, diff.seconds / 3600 )

    cg =  CalculateGpm( email, ini_date, end_date, filepath_csv )
    r = cg.init()
    if not r['isOk']:
        print( r['message'])
        return 0

    dtIni = datetime.now()
    print('Started ', dtIni)
    cg.saveCsv( download_keep )
    dtEnd = datetime.now()
    msgDiff = messageDiffDateTime( dtIni, dtEnd )
    print('\nFinished ', f"{dtEnd}({msgDiff})")
    return 0

def main():
    parser = argparse.ArgumentParser(description=f"Create precipitation daily from NASA/GPM ({GpmDataset.HOST})." )
    parser.add_argument( 'email', action='store', help='Email user for NASA/GPM', type=EmailType('RFC5322'))
    parser.add_argument( 'ini_date', action='store', help='Initial date (YYYY-mm-DD)', type=str)
    parser.add_argument( 'end_date', action='store', help='End date (YYYY-mm-DD)', type=str)
    parser.add_argument( 'filepath_csv', action='store', help='Filepath of CSV with coordinates of stations', type=str)
    parser.add_argument( '-d', '--download_keep', action="store_true", help='Keep downloads')

    args = parser.parse_args()
    return run( args.email, args.ini_date, args.end_date, args.filepath_csv, args.download_keep )

if __name__ == "__main__":
    sys.exit( main() )
