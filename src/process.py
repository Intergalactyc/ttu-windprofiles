# ruff: noqa: F403, F405
from definitions import *
from config import parse, results_dir
import windprofiles.process as process
import windprofiles.process.compute as compute
import windprofiles.process.sonic as sonic
import windprofiles.lib.atmos as atmos
import pandas as pd
import numpy as np
import warnings
import os
warnings.filterwarnings("ignore", message = "DataFrame is highly fragmented")


# TODO: check the units - is pressure really originally in inHg? because converted results are ~85-90 which is ~0.9 atm


def get_datetime_from_filename(filepath: os.PathLike) -> pd.Timestamp:
    filename = os.path.basename(filepath).split(".")[0]
    DATE_STR = filename.split("_")[4]
    YEAR = int(DATE_STR[1:5])
    MONTH = int(DATE_STR[5:7])
    DAY = int(DATE_STR[7:9])
    TIME_STR = filename.split("_")[5]
    HOUR = int(TIME_STR[1:3])
    MIN = int(TIME_STR[3:5])
    START_TIME = pd.Timestamp(year = YEAR, month = MONTH, day = DAY, hour = HOUR, minute = MIN, tz = "UTC")
    return START_TIME


def load_and_format_file(filepath: os.PathLike) -> tuple[pd.DataFrame, list[int]]:
    df = pd.read_csv(filepath, compression = "gzip", header = None, engine = "pyarrow")
    df.rename(columns = {i : SOURCE_HEADERS[i] for i in range(120)}, inplace = True)
    df.drop(columns = [head for head in SOURCE_HEADERS if int(head.split("_")[1]) in DROP_BOOMS], inplace = True)

    df = process.rename_headers(df, HEADER_MAP, True, True)

    boomset = set()
    for col in df.columns:
        col_type, boom_number = col.split("_")
        boomset.add(int(boom_number))
    booms_list = list(boomset)
    booms_list.sort()

    return df, booms_list


def summarize_df(df: pd.DataFrame, booms_available: list[int], timestamp: pd.Timestamp) -> dict:
    result = {"time" : timestamp}

    # TODO: streamwise coordinate alignment
    # TODO: stationarity testing
    # TODO: autocorrelations -> integral scales

    for b in booms_available:
        df[f"vpt_{b}"] = df.apply(lambda row : atmos.vpt_from_3(row[f"rh_{b}"], row[f"p_{b}"], row[f"t_{b}"]), axis = 1)

    result |= sonic.get_stats(df, np.mean, "_mean", ["u", "v", "w", "ws", "wd", "t", "ts", "vpt", "rh", "p"])

    for var in ["u","v","w","vpt"]: # Get Reynolds deviations
        for b in booms_available:
            df[f"{var}'_{b}"] = df[f"{var}_{b}"] - result[f"{var}_{b}_mean"]
    
    for var in ["u","v","vpt"]: # Get vertical fluxes
        for b in booms_available:
            df[f"w'{var}'_{b}"] = df[f"w'_{b}"] * df[f"{var}'_{b}"]

    result |= sonic.get_stats(df, np.std, "_std", ["u", "v", "w", "ws", "wd"])

    result |= sonic.get_stats(df, np.mean, "_mean", ["w'u'", "w'v'", "w'vpt'"])

    # Turbulence intensity (this and following can be done based on summary stats alone, so could be transferred to later step)
    for b in booms_available:
        result[f"ti_{b}"] = result[f"ws_{b}_std"] / result[f"ws_{b}_mean"]

    # TODO: compute L, Ri_b, Ri_f, alpha, u*
        # Try getting these with different heights (/pairs for Ri_b) to see sensitivity to choice of height

    return result


def qc_step(df):
    # Rolling outlier removal
    df, elims = process.rolling_outlier_removal(df = df,
                                            window_size_observations = OUTLIER_REMOVAL_WINDOW,
                                            sigma = OUTLIER_REMOVAL_SIGMA,
                                            column_types = ["u", "v", "t", "ts", "p", "rh"],
                                            remove_if_any = False)

    for key, val in elims.items():
        if val > ROWS_PER_FILE*0.02:
            print(f"For {filepath}, more than 2% ({val}) of {key} removed as spikes")

    return df


def process_file(filepath: os.PathLike, qc: bool = True) -> list[dict]:
    df, booms_available = load_and_format_file(filepath)

    # Unit conversion
    df = process.convert_dataframe_units(df, from_units = SOURCE_UNITS, gravity = LOCATION.g)
    
    # QC step
    if qc:
        df = qc_step(df)

    return df, booms_available


def summarize_file(filepath: os.PathLike) -> list[dict]:
    df, booms_available = process_file(filepath)

    TIMESTAMP = get_datetime_from_filename(filepath).tz_convert(LOCATION.timezone)

    split = [df.iloc[CHUNK_SIZE*i:CHUNK_SIZE*(i+1)] for i in range(SPLIT_INTO_CHUNKS)]

    result = [summarize_df(d, booms_available, TIMESTAMP + i * pd.Timedelta(CHUNK_TIME, "min")) for i, d in enumerate(split)]

    return result


def process_day_directory(dirpath: os.PathLike, nproc: int, test: bool) -> pd.DataFrame:
    df = sonic.analyze_directory(
        path=dirpath,
        analysis=summarize_file,
        nproc=nproc,
        limit=nproc if test else None,
        index="time",
        progress=True
    )
    df.reset_index(names="time", inplace=True)
    df["time"] = pd.to_datetime(df["time"])
    df.set_index("time", inplace=True)
    df.sort_index(ascending=True, inplace=True)
    return df


def main():
    args = parse("process")
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
        dfs = []
        for d in days:
            day_dir = os.path.join(dirpath,d)
            try:
                df = process_day_directory(day_dir, args["nproc"], args["test"])
            except Exception as e:
                print(f"Encountered error for day {d} in {day_dir}: {e}")
            else:
                dfs.append(df)
            if args["test"]:
                break
        res = pd.concat(dfs, axis=0)
        res.to_csv(os.path.join(results_dir, "testing" if args["test"] else "processed", f"{k}.csv"), float_format="%g")
        if args["test"]:
            break

if __name__ == "__main__":
    main()
