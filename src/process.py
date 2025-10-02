# ruff: noqa: F403, F405
from definitions import *
from config import parse
import windprofiles.process as process
import windprofiles.process.sonic as sonic
import pandas as pd
import numpy as np
import warnings
import pathlib
import os
warnings.filterwarnings('ignore', message = "DataFrame is highly fragmented")


day_results_path = pathlib.Path(__file__).parent.parent.joinpath("results/processed")


def get_datetime_from_filename(filepath: str):
    filename = os.path.basename(filepath).split(".")[0]
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

    df = process.rename_headers(df, HEADER_MAP, True, True)

    boomset = set()
    for col in df.columns:
        col_type, boom_number = col.split('_')
        boomset.add(int(boom_number))
    booms_list = list(boomset)
    booms_list.sort()

    return df, booms_list


def process_file(filepath):
    df, booms_available = load_and_format_file(filepath)

    # Unit conversion
    df = process.convert_dataframe_units(df, from_units = SOURCE_UNITS, gravity = LOCATION.g, silent = True)
    
    # Rolling outlier removal
    df, elims = process.rolling_outlier_removal(df = df,
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


def summarize_file(filepath):
    # To split (30min) file into several (10min) chunks, use a .resample() trick and then return a list of the summary dicts
    df, booms_available = process_file(filepath)

    TIMESTAMP = get_datetime_from_filename(filepath).tz_convert(LOCATION.timezone)
    result = {'time' : TIMESTAMP}

    result |= sonic.get_stats(df, np.mean, '', ['u', 'v', 't', 'ts', 'rh', 'p', 'wd'])
    # next compute fluxes

    return result

def process_day_directory(dirpath, savename, nproc):
    df = sonic.analyze_directory(
        path=dirpath,
        analysis=summarize_file,
        nproc=nproc,
        index="time",
        progress=True
    )
    df.reset_index(names="time", inplace=True)
    df["time"] = pd.to_datetime(df["time"])
    df.set_index("time", inplace=True)
    df.sort_index(ascending=True, inplace=True)
    df.to_csv(os.path.join(day_results_path, savename), float_format="%g")


def main():
    args = parse()
    if (n:=args.get("nproc")) is None:
        args["nproc"] = NPROC
    elif not isinstance(n, int) or n < 1:
        raise ValueError(f"Invalid argument --nproc={n}")

    dirs = args["process"]
    if (o := args.get("only")) is not None:
        if o not in dirs:
            raise KeyError(f"{o} not in process keys")
        dirs = {o : dirs.get(o)}
    
    for k, v in dirs.items():
        dirpath = os.path.join(args["data"], v)
        days = os.listdir(dirpath)
        for d in days:
            day_dir = os.path.join(dirpath,d)
            savename = f"{d}{k}.csv"
            process_day_directory(day_dir, savename, args["nproc"])

if __name__ == "__main__":
    main()
