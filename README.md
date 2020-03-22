## Scripts for work with [Global Precipitation Measurement (GPM)](https://pps.gsfc.nasa.gov/)

### Daily precipitation GPM (daily_precipitation_gpm.py): 
- Dependencies:
  - Python 3
  - gdal(>= 2.4) osgeo package
- Create a CSV file with values of daily precipitation from FTP (arthurhou.pps.eosdis.nasa.gov)
  - CSV Fields:  ID station | date | total_mm(APD)
  - APD (Accumulation Precipitation of Day): Previous day(12:00 to 23:59) + Current day(00:00 to 11:59)
- Arguments:
  - email: It is the user and password for FTP
  - ini_date: Initial date (YYYY-mm-DD)
  - end_date: End date (YYYY-mm-DD)
  - filepath_csv: Filepath of CSV with coordinates of stations(delimiter semicolon)  
    - Coordinate Reference System: EPSG 4326  
    - Example:  
    ID;LAT;LONG  
    A354;-6.974135;-42.146831  
    A364;-9.875196;-45.345805  
    ...
  - download_keep(-d) - OPTIONAL: Keep download images
- Process:
  - Download of images to one day(48 images)
    - Hours of previous day: 12:00 to 23:59 (24 images)
    - Hours of current day:  00:00 to 11:59 (24 images)
  - Reads the pixel value using the coordinates of the stations  
    For one station reads 48 images
  - Sum all pixel values for each station and divide by 20  
    20 is a factor using to convert for mm/Day
  - Create daily precipitation CSV(delimiter semicolon)  
    CSV Name is filepath_csv (argument) with 'gpm' suffix
    - Example:  
    id;date;total_mm  
    A354;2019-02-01;0.0  
    A364;2019-02-01;0.1  
    ...  
  - Use:  
  ```
  python3 daily_precipitation_gpm.py -h
  python3 daily_precipitation_gpm.py test@gmail.com 2019-02-02 2019-02-05 station_cerrado.csv
  python3 daily_precipitation_gpm.py test@gmail.com 2019-02-02 2019-02-05 station_cerrado.csv -d
  ```
  
