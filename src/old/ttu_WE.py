import windprofiles.sonic as sonic
import windprofiles.process.format as fmt
import windprofiles.process.
import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore', message = "DataFrame is highly fragmented")


SOURCE_DIRECTORY = 'C:/Users/22wal/Documents/GLWind/data/200-m MetTower Data/200m-2018-12/tower/procd/2018/Dec'
OUTPUT_DIRECTORY = 'C:/Users/22wal/Documents/GLWind/CE5348_TTUNWIdata'

# Local gravity at TTU (latitude ~ 33.59 degrees, elevation ~ 1014 m), in m/s^2
LOCAL_GRAVITY = 9.793

# Latitude and longitude of TTU 200-meter tower, each in degrees
LATITUDE = 33.59
LONGITUDE = -102.03
ELEVATION_METERS = 1014

SOURCE_TIMEZONE = 'UTC'
LOCAL_TIMEZONE = 'US/Central'

NPROC = 12

SOURCE_UNITS = {
    'p' : 'inHg',
    't' : 'F',
    'ts' : 'F',
    'rh' : '%',
    'u' : 'mph',
    'v' : 'mph',
    'w' : 'mph',
    'ws' : 'mph',
    'wd' : ('degrees', 'N', 'CW')
}

SOURCE_HEADERS = ['TSU_1', 'TSV_1', 'TSW_1', 'TST_1', 'TT_1', 'TRH_1', 'TBP_1', 'TSU_2', 'TSV_2', 'TSW_2', 'TST_2', 'TT_2', 'TRH_2', 'TBP_2', 'TSU_3', 'TSV_3', 'TSW_3', 'TST_3', 'TT_3', 'TRH_3', 'TBP_3', 'TSU_4', 'TSV_4', 'TSW_4', 'TST_4', 'TT_4', 'TRH_4', 'TBP_4', 'TSU_5', 'TSV_5', 'TSW_5', 'TST_5', 'TT_5', 'TRH_5', 'TBP_5', 'TSU_6', 'TSV_6', 'TSW_6', 'TST_6', 'TT_6', 'TRH_6', 'TBP_6', 'TSU_7', 'TSV_7', 'TSW_7', 'TST_7', 'TT_7', 'TRH_7', 'TBP_7', 'TSU_8', 'TSV_8', 'TSW_8', 'TST_8', 'TT_8', 'TRH_8', 'TBP_8', 'TSU_9', 'TSV_9', 'TSW_9', 'TST_9', 'TT_9', 'TRH_9', 'TBP_9', 'TSU_10', 'TSV_10', 'TSW_10', 'TST_10', 'TT_10', 'TRH_10', 'TBP_10', 'TSN-TRANS_1', 'TSW-TRANS_1', 'TSV-TRANS_1', 'TS-WS_1', 'TS-WD_1', 'TSN-TRANS_2', 'TSW-TRANS_2', 'TSV-TRANS_2', 'TS-WS_2', 'TS-WD_2', 'TSN-TRANS_3', 'TSW-TRANS_3', 'TSV-TRANS_3', 'TS-WS_3', 'TS-WD_3', 'TSN-TRANS_4', 'TSW-TRANS_4', 'TSV-TRANS_4', 'TS-WS_4', 'TS-WD_4', 'TSN-TRANS_5', 'TSW-TRANS_5', 'TSV-TRANS_5', 'TS-WS_5', 'TS-WD_5', 'TSN-TRANS_6', 'TSW-TRANS_6', 'TSV-TRANS_6', 'TS-WS_6', 'TS-WD_6', 'TSN-TRANS_7', 'TSW-TRANS_7', 'TSV-TRANS_7', 'TS-WS_7', 'TS-WD_7', 'TSN-TRANS_8', 'TSW-TRANS_8', 'TSV-TRANS_8', 'TS-WS_8', 'TS-WD_8', 'TSN-TRANS_9', 'TSW-TRANS_9', 'TSV-TRANS_9', 'TS-WS_9', 'TS-WD_9', 'TSN-TRANS_10', 'TSW-TRANS_10', 'TSV-TRANS_10', 'TS-WS_10', 'TS-WD_10']

HEADER_MAP = {
    'TSU' : None,
    'TSV' : None,
    'TSW' : None,
    'TST' : 'ts',
    'TT' : 't',
    'TRH' : 'rh',
    'TBP' : 'p',
    'TSN-TRANS' : 'u',
    'TSW-TRANS' : 'v',
    'TSV-TRANS' : 'w',
    'TS-WS' : 'ws',
    'TS-WD' : 'wd'
}

HEADER_MAP_INV = {v : k for k, v in HEADER_MAP.items()}

BOOMS_LIST = list(range(1, 11))
HEIGHTS_LIST = [0.9, 2.4, 4.0, 10.1, 16.8, 47.3, 74.7, 116.5, 158.2, 200.0]
HEIGHTS = {b : h for b, h in zip(BOOMS_LIST, HEIGHTS_LIST)}
DROP_BOOMS = [8, 10]

OUTLIER_REMOVAL_WINDOW = 60*50*5 # records; = 5 minutes
OUTLIER_REMOVAL_SIGMA = 5

def get_datetime_from_filename(filepath: str):
    filename = filepath.split('/')[-1]
    DATE_STR = filename.split('_')[4]
    YEAR = int(DATE_STR[1:5])
    MONTH = int(DATE_STR[5:7])
    DAY = int(DATE_STR[7:9])
    TIME_STR = filename.split('_')[5]
    HOUR = int(TIME_STR[1:3])
    MIN = int(TIME_STR[3:5])
    START_TIME = pd.Timestamp(year = YEAR, month = MONTH, day = DAY, hour = HOUR, minute = MIN, tz = 'UTC')
    return START_TIME

def load_and_format_file(filename):
    df = pd.read_csv(filename, compression = 'gzip', header = None, engine = 'pyarrow')
    df.rename(columns = {i : SOURCE_HEADERS[i] for i in range(120)}, inplace = True)
    df.drop(columns = [head for head in SOURCE_HEADERS if int(head.split('_')[1]) in DROP_BOOMS], inplace = True)

    df = preprocess.rename_headers(df, HEADER_MAP, True, True)

    boomset = set()
    for col in df.columns:
        col_type, boom_number = col.split('_')
        boomset.add(int(boom_number))
    booms_list = list(boomset)
    booms_list.sort()

    return df, booms_list

def summarize_file(filepath):
    df, booms_available = process_file(filepath)

    if booms_available != [1, 2, 3, 4, 5, 6, 7, 9]:
        print(f'Warning - {filepath} had booms {booms_available}')

    # Leaving timestamp in UTC
    TIMESTAMP = get_datetime_from_filename(filepath).tz_convert(LOCAL_TIMEZONE)
    result = {'time' : TIMESTAMP}

    result |= sonic.get_stats(df, np.mean, '', ['u', 'v', 't', 'ts', 'rh', 'p', 'wd'])

    return result

def process_file(filepath):
    df, booms_available = load_and_format_file(filepath)

    # Unit conversion
    df = preprocess.convert_dataframe_units(df, from_units = SOURCE_UNITS, gravity = LOCAL_GRAVITY, silent = True)
    
    # Rolling outlier removal
    df, elims = preprocess.rolling_outlier_removal(df = df,
                                            window_size_observations = OUTLIER_REMOVAL_WINDOW,
                                            sigma = OUTLIER_REMOVAL_SIGMA,
                                            column_types = ['u', 'v', 't', 'ts', 'p', 'rh'],
                                            silent = True,
                                            remove_if_any = False,
                                            return_elims = True)
    
    for key, val in elims.items():
        if val > 50*60*30*0.02:
            print(f'For {filepath}, more than 2% ({val}) of {key} removed as spikes')

    return df, booms_available

def process_day(day: int, short: bool = False):
    return sonic.analyze_directory(path = f'{SOURCE_DIRECTORY}/{str(day).zfill(2)}',
                                      analysis = summarize_file,
                                      nproc = NPROC,
                                      index = 'time',
                                      limit = NPROC if short else None,
                                      progress = True)

def run_sonic_processing(short: bool = False):
    for i in range(1, 2 if short else 8):
        day_summary = process_day(day = i, short = short)
        day_summary.reset_index(names = 'time', inplace = True)
        day_summary['time'] = pd.to_datetime(day_summary['time'])
        day_summary.set_index('time', inplace = True)
        day_summary.sort_index(ascending = True, inplace = True) # in case multiprocessing put it out of order
        day_summary.to_csv(f'{OUTPUT_DIRECTORY}/2018Dec{str(i).zfill(2)}_30min.csv', float_format = '%g')

if __name__ == '__main__':
    import sys
    short = False
    if '-s' in sys.argv:
        short = True
    run_sonic_processing(short)
