# ruff: noqa: F403,F405,E402

import windprofiles.sonic as sonic
import windprofiles.preprocess as preprocess
import windprofiles.lib.atmos as atmos
import windprofiles.compute as compute
from windprofiles.lib.other import zeropad
import matplotlib
matplotlib.use('Qt5Agg')
import matplotlib.pyplot as plt
import matplotlib.style as mplstyle
mplstyle.use('fast')
from definitions import *
import warnings
warnings.filterwarnings('ignore', message = "DataFrame is highly fragmented")

PERIOD = 30 # minutes, right now does nothing

WE_RULES = {
    # Right now does nothing
}

NPROC = 12
LIMIT = None
SHORT = False
REPROCESS = False

OUTPUT_FILE = f'{OUTPUT_DIRECTORY}/data{PERIOD}min.csv'

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

def load_and_format_file(filename, restrict: bool = False, vars: list = None, booms: list = None):
    df = pd.read_csv(filename, compression = 'gzip', header = None, engine = 'pyarrow')
    df.rename(columns = {i : SOURCE_HEADERS[i] for i in range(120)}, inplace = True)

    if restrict:
        cols = df.columns
        df.drop(columns = [col for col in cols if int(col.split('_')[-1]) not in booms])
        try:
            restrictmap = {HEADER_MAP_INV[var] : var for var in vars}
        except Exception:
            restrictmap = HEADER_MAP

    df = preprocess.rename_headers(df, restrictmap if restrict else HEADER_MAP, True, True)

    if not restrict:
        boomset = set()
        for col in df.columns:
            col_type, boom_number = col.split('_')
            boomset.add(int(boom_number))
        booms_list = list(boomset)
        booms_list.sort()

    return df, booms if restrict else booms_list

def summarize_file(args, df_single_var: str = None):
    filepath, rules = args

    df, booms_available = process_file(filepath, rules)

    TIMESTAMP = get_datetime_from_filename(filepath).tz_convert(LOCAL_TIMEZONE)
    result = {'time' : TIMESTAMP}

    result |= sonic.get_stats(df, np.mean, '_mean', ['u', 'v', 'w', 'ws', 't', 'ts', 'vpt', 'rh', 'p', 'wdDel'])

    for var in ['u','v','w','vpt']: # Get Reynolds deviations
        for b in booms_available:
            df[f"{var}'_{b}"] = df[f'{var}_{b}'] - result[f'{var}_{b}_mean']
    
    for var in ['u','v','vpt']: # Get vertical fluxes
        for b in booms_available:
            df[f"w'{var}'_{b}"] = df[f"w'_{b}"] * df[f"{var}'_{b}"]
        if df_single_var == f"w'{var}'":
            return df

    result |= sonic.get_stats(df, np.std, '_std', ['u', 'v', 'w', 'ws'])

    result |= sonic.get_stats(df, np.mean, '_mean', ["w'u'", "w'v'", "w'vpt'"])

    # h_lowest = min(booms_available)
    # b_lowest = booms_available.index(h_lowest)
    
    # What about aligning to mean wind direction?

    return result

def process_file(filepath, rules = None, restrict = False, vars = None, booms = None):
    df, booms_available = load_and_format_file(filepath, restrict = restrict, vars = vars, booms = booms)

    df = preprocess.convert_dataframe_units(df, from_units = SOURCE_UNITS, gravity = LOCAL_GRAVITY, silent = True)

    if not restrict or 'vpt' in vars:
        for b in booms_available:
            df[f'vpt_{b}'] = df.apply(lambda row : atmos.vpt_from_3(row[f'rh_{b}'], row[f'p_{b}'], row[f't_{b}']), axis = 1)
    
    if not restrict or 'ws' in vars:
        for b in booms_available:
            df[f'ws_{b}'] = np.sqrt(df[f'u_{b}']**2 + df[f'v_{b}']**2) 
    # if restrict is False:
    #     for b in booms_available:
    #         df[f'wdX_{b}'] = np.rad2deg(np.arctan2(-df[f'v_{b}'], df[f'u_{b}'])) #(np.rad2deg(np.arctan2(df[f'u_{b}'], df[f'v_{b}'])) - 90.) % 360
    #         df[f'wdDel_{b}'] = df[f'wdX_{b}'] - df[f'wd_{b}']

    return df, booms_available

def process_day(day, rules):
    return sonic.analyze_directory(path = f'{SOURCE_DIRECTORY}/{zeropad(day,2)}',
                                      analysis = summarize_file,
                                      rules = rules,
                                      nproc = NPROC,
                                      index = 'time',
                                      limit = LIMIT,
                                      progress = True)

def run_sonic_processing():
    day_summaries = []
    for i in range(1, 2 if SHORT else 16):
        day_summaries.append(process_day(day = i, rules = WE_RULES))
    period_summary = pd.concat(day_summaries)
    period_summary.reset_index(names = 'time', inplace = True)
    period_summary['time'] = pd.to_datetime(period_summary['time'])
    period_summary.set_index('time', inplace = True)
    period_summary.sort_index(ascending = True, inplace = True)
    period_summary.to_csv(OUTPUT_FILE, float_format = '%g') # can use %.{n}g for n-sigfig

def run_computations(filename = OUTPUT_FILE, cut = False) -> pd.DataFrame:
    df = pd.read_csv(filename)
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace = True)

    # bulk Ri between booms 5 and 6 (16.8 and 47.3 meters)
    df = compute.bulk_richardson_number(df, [5, 6], [16.8, 47.3], silent = True, gravity = LOCAL_GRAVITY, components = True, suffix = '_mean', colname = 'ri_bulk')

    if not cut:
        for boom in BOOMS_LIST:
            df[f'ti_{boom}'] = df[f'ws_{boom}_std'] / df[f'ws_{boom}_mean']

        SAFE_BOOMS = [1,2,3,4,5,6,7,9]
        SAFE_HEIGHTS = [HEIGHTS[b] for b in SAFE_BOOMS]

        df = compute.power_law_fits(df, SAFE_BOOMS, SAFE_HEIGHTS, 4, [None, 'alpha'], silent = True, suffix = '_mean')
    
    return df

def generate_plots(df: pd.DataFrame):
    for boom in BOOMS_LIST:
        plt.plot(df.index, df[f't_{boom}_mean'], label = f'Temperature at {HEIGHTS[boom]} meters')
    plt.legend()
    plt.show()

# Generate plot of slow data such that we can click on any data point and generate a plot of the underlying sonic data for that variable
# Start with just scatters

def get_sonic_from_timestamp(ts: datetime, variable: str, boom: int): # with restriction
    time = ts.astimezone('UTC')
    daystr = zeropad(str(time.day), 2)
    for file in os.listdir(f'{SOURCE_DIRECTORY}/{daystr}'):
        file_ts = get_datetime_from_filename(file)
        if file_ts == time:# or np.abs(time - file_ts) < epsilon:
            if "w'" in variable:
                return summarize_file((f'{SOURCE_DIRECTORY}/{daystr}/{file}',WE_RULES), df_single_var = variable)
            elif variable == "ws":
                return process_file(f'{SOURCE_DIRECTORY}/{daystr}/{file}', rules = WE_RULES)[0]
            else:
                return process_file(f'{SOURCE_DIRECTORY}/{daystr}/{file}', rules = WE_RULES, restrict = True, vars = [variable], booms = [boom])[0]

def interactive_plot(df, variable, booms):
    fig, ax = plt.subplots(figsize = (12, 8))
    fig.canvas.manager.set_window_title(f'{FIGVARS[variable]} averaged data')

    x = df.index

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    booms_by_artists = {}
    legend_elements = []
    for b, c in zip(booms, colors):
        y = df[f'{variable}_{b}_mean']
        scatter = ax.scatter(x, y, s = 4, picker = True)
        booms_by_artists[scatter] = b
        legend_elements.append(Patch(facecolor = c, edgecolor = c, label = f'{FIGVARS[variable]}, boom {b} ({HEIGHTS[b]} m)'))

    artists_from_legend = {}
    legend = ax.legend(handles = legend_elements, fancybox = True, shadow = True)
    legend.set_draggable(True)

    for legend_artist, boom_artist in zip(legend.get_patches(), booms_by_artists.keys()):
        legend_artist.set_picker(True)
        artists_from_legend[legend_artist] = boom_artist

    ax.set_ylabel(f'{FIGVARS[variable]} ({FIGUNITS[variable]})')
    ax.set_xlabel('time')

    def onpick(event):
        artist = event.artist
        if artist in booms_by_artists.keys():
            boom = booms_by_artists[artist]
            timestamp = x[event.ind][0]
            print(f'Loading sonic {FIGVARS[variable]} data for boom {boom} at {timestamp}')
            sonic_subplot(get_sonic_from_timestamp(timestamp, variable, boom), variable, boom, timestamp)
        elif artist in artists_from_legend.keys():
            boom_artist = artists_from_legend[artist]
            button = event.mouseevent.button
            if button == MouseButton.LEFT:
                visible = not boom_artist.get_visible()
                boom_artist.set_visible(visible)
                artist.set_alpha(1.0 if visible else 0.2)
            elif button == MouseButton.RIGHT:
                boom_artist.set_visible(True)
                artist.set_alpha(1.0)
                for Lart, Bart in artists_from_legend.items():
                    if Lart != artist:
                        Bart.set_visible(False)
                        Lart.set_alpha(0.2)
            fig.canvas.draw()

    def onkey(event):
        if event.key == ' ': # Set all visible on spacebar press
            for Lart, Bart in artists_from_legend.items():
                Bart.set_visible(True)
                Lart.set_alpha(1.0)
            fig.canvas.draw()

    fig.canvas.mpl_connect('pick_event', onpick)
    fig.canvas.mpl_connect('key_press_event', onkey)

    plt.show(block = False)

def sonic_subplot(dfs, variable, boom, time):
    fig, ax = plt.subplots(figsize = (10, 7))
    fig.canvas.manager.set_window_title(f'Sonic {variable}, boom {boom}, {time}')

    ax.plot(dfs.index, dfs[f'{variable}_{boom}'], linewidth = 1)

    ax.set_title(f'{FIGVARS[variable]}, boom {boom} ({HEIGHTS[boom]} meters)')
    ax.set_xlabel(f'collections since {time}')
    ax.set_ylabel(f'{FIGVARS[variable]} ({FIGUNITS[variable]})')

    plt.show(block = False)

def normal_plot(df, variable, booms): # right now assume dimless
    fig, ax = plt.subplots(figsize = (12, 8))

    fig.canvas.manager.set_window_title(f'{NIFIGVARS[variable]} data')

    if variable == 'ti':
        # for b in booms:
        #     ax.scatter(df.index, df[f'ti_{b}'], s = 4, label = f'Boom {b} ({HEIGHTS[b]}m)')
        # ax.legend()

        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

        booms_by_artists = {}
        legend_elements = []
        for b, c in zip(booms, colors):
            y = df[f'ti_{b}']
            scatter = ax.scatter(df.index, y, s = 4, picker = True)
            booms_by_artists[scatter] = b
            legend_elements.append(Patch(facecolor = c, edgecolor = c, label = f'Boom {b} ({HEIGHTS[b]}m)'))

        artists_from_legend = {}
        legend = ax.legend(handles = legend_elements, fancybox = True, shadow = True)
        legend.set_draggable(True)

        for legend_artist, boom_artist in zip(legend.get_patches(), booms_by_artists.keys()):
            legend_artist.set_picker(True)
            artists_from_legend[legend_artist] = boom_artist

        def onpick(event):
            artist = event.artist
            if artist in artists_from_legend.keys():
                boom_artist = artists_from_legend[artist]
                button = event.mouseevent.button
                if button == MouseButton.LEFT:
                    visible = not boom_artist.get_visible()
                    boom_artist.set_visible(visible)
                    artist.set_alpha(1.0 if visible else 0.2)
                elif button == MouseButton.RIGHT:
                    boom_artist.set_visible(True)
                    artist.set_alpha(1.0)
                    for Lart, Bart in artists_from_legend.items():
                        if Lart != artist:
                            Bart.set_visible(False)
                            Lart.set_alpha(0.2)
                fig.canvas.draw()

        def onkey(event):
            if event.key == ' ': # Set all visible on spacebar press
                for Lart, Bart in artists_from_legend.items():
                    Bart.set_visible(True)
                    Lart.set_alpha(1.0)
                fig.canvas.draw()

        fig.canvas.mpl_connect('pick_event', onpick)
        fig.canvas.mpl_connect('key_press_event', onkey)

    else:
        ax.scatter(df.index, df[variable], s = 5)

    ax.set_xlabel('Time')
    ax.set_ylabel(NIFIGVARS[variable])
    plt.show(block = False)

def interact_CLI(df): # DOES NOT CURRENTLY WORK (b/c process w/o summarize doesn't generate) FOR FLUX QUANTITIES 
    print('Entered interactive plotting mode. Respond to an input with QUIT to exit, HELP to see variables, or TABLE to print data.')
    while True:
        user_in = input('Enter name of variable to plot: ').strip().lower()
        if user_in in FIGVARS.keys():
            print(f'Plotting {FIGVARS[user_in]}.')
            interactive_plot(df, user_in, BOOMS_LIST)
        elif user_in in NIFIGVARS.keys():
            normal_plot(df, user_in, BOOMS_LIST)
        elif user_in in ['quit', 'exit']:
            break
        elif user_in in ['help', 'vars']:
            print(f'Available variables: {FIGVARS}')
        elif user_in in ['table', 'df', 'data']:
            print(df)
        else:
            print(f'Unrecognized variable "{user_in}".')
    
if __name__ == '__main__':
    if REPROCESS:
        run_sonic_processing()

    df = run_computations(filename = f'{OUTPUT_DIRECTORY}/data{PERIOD}min_past.csv', cut = True)

    interact_CLI(df)
