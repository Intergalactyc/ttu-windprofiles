# ruff: noqa: F403, F405, E402
from definitions import *
from config import parse, results_dir
from process import process_file, get_datetime_from_filename
from datetime import datetime
import pandas as pd
import matplotlib
matplotlib.use('Qt5Agg')
import matplotlib.pyplot as plt
import matplotlib.style as mplstyle
mplstyle.use('fast')
from matplotlib.patches import Patch
from matplotlib.backend_bases import MouseButton

def get_sonic_from_timestamp(ts: datetime, rawpath: os.PathLike, qc: bool):
    time = ts.astimezone("UTC")
    offset = time.minute % 30
    corrected = time - pd.Timedelta(offset, "min")

    daystr = str(corrected.day).zfill(2)
    parent = os.path.join(rawpath, daystr)

    for file in os.listdir(parent):
        file_ts = get_datetime_from_filename(file)
        if file_ts == corrected:
            filepath = os.path.join(parent, file)
            return process_file(filepath, qc)[0].iloc[offset*50*60:(offset+CHUNK_TIME)*50*60]

def interactive_plot(df: pd.DataFrame, variable: str, booms: list[int], rawpath: os.PathLike):
    fig, ax = plt.subplots(figsize = (12, 8))
    fig.canvas.manager.set_window_title(f"{FIGVARS[variable]} averaged data")

    x = df.index

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
    booms_by_artists = {}
    legend_elements = []
    for b, c in zip(booms, colors):
        y = df[f"{variable}_{b}_mean"]
        scatter = ax.scatter(x, y, s = 4, picker = True)
        booms_by_artists[scatter] = b
        legend_elements.append(Patch(facecolor = c, edgecolor = c, label = f"{FIGVARS[variable]}, boom {b} ({HEIGHTS_DICT[b]} m)"))

    artists_from_legend = {}
    legend = ax.legend(handles = legend_elements, fancybox = True, shadow = True)
    legend.set_draggable(True)

    for legend_artist, boom_artist in zip(legend.get_patches(), booms_by_artists.keys()):
        legend_artist.set_picker(True)
        artists_from_legend[legend_artist] = boom_artist

    ax.set_ylabel(f"{FIGVARS[variable]} ({FIGUNITS[variable]})")
    ax.set_xlabel("time")

    def onpick(event):
        artist = event.artist
        button = event.mouseevent.button
        if artist in booms_by_artists.keys():
            qc = (button == MouseButton.LEFT)
            boom = booms_by_artists[artist]
            timestamp = x[event.ind][0]
            print(f"Loading sonic {FIGVARS[variable]} data for boom {boom} at {timestamp}")
            sonic_subplot(get_sonic_from_timestamp(timestamp, rawpath, qc), variable, boom, timestamp, qc)
        elif artist in artists_from_legend.keys():
            boom_artist = artists_from_legend[artist]
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
        if event.key == " ": # Set all visible on spacebar press
            for Lart, Bart in artists_from_legend.items():
                Bart.set_visible(True)
                Lart.set_alpha(1.0)
            fig.canvas.draw()

    fig.canvas.mpl_connect("pick_event", onpick)
    fig.canvas.mpl_connect("key_press_event", onkey)

    plt.show(block = False)

def sonic_subplot(dfs, variable, boom, time, qc):
    fig, ax = plt.subplots(figsize = (10, 7))
    fig.canvas.manager.set_window_title(f"Sonic {variable}, boom {boom}, {time}")

    ax.plot(dfs.index, dfs[f"{variable}_{boom}"], linewidth = 1)

    ax.set_title(f"{FIGVARS[variable]}, boom {boom} ({HEIGHTS_DICT[boom]} meters)" + (" *NO QC*" if not qc else ""))
    ax.set_xlabel(f"collections since {time}")
    ax.set_ylabel(f"{FIGVARS[variable]} ({FIGUNITS[variable]})")

    plt.show(block = False)

def normal_plot(df, variable, booms): # right now assume dimless
    fig, ax = plt.subplots(figsize = (12, 8))

    fig.canvas.manager.set_window_title(f"{NIFIGVARS[variable]} data")

    if variable == "ti":
        # for b in booms:
        #     ax.scatter(df.index, df[f"ti_{b}"], s = 4, label = f"Boom {b} ({HEIGHTS_DICT[b]}m)")
        # ax.legend()

        colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]

        booms_by_artists = {}
        legend_elements = []
        for b, c in zip(booms, colors):
            y = df[f"ti_{b}"]
            scatter = ax.scatter(df.index, y, s = 4, picker = True)
            booms_by_artists[scatter] = b
            legend_elements.append(Patch(facecolor = c, edgecolor = c, label = f"Boom {b} ({HEIGHTS_DICT[b]}m)"))

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
            if event.key == " ": # Set all visible on spacebar press
                for Lart, Bart in artists_from_legend.items():
                    Bart.set_visible(True)
                    Lart.set_alpha(1.0)
                fig.canvas.draw()

        fig.canvas.mpl_connect("pick_event", onpick)
        fig.canvas.mpl_connect("key_press_event", onkey)

    else:
        ax.scatter(df.index, df[variable], s = 5)

    ax.set_xlabel("Time")
    ax.set_ylabel(NIFIGVARS[variable])
    plt.show(block = False)

def interact_CLI(df: pd.DataFrame, rawpath: os.PathLike): # DOES NOT CURRENTLY WORK (b/c process w/o summarize doesn't generate) FOR FLUX QUANTITIES 
    print("Entered interactive plotting mode. Respond to an input with QUIT to exit, HELP to see variables, or TABLE to print data.")
    while True:
        try:
            user_in = input("Enter name of variable to plot: ").strip().lower()
            if user_in in FIGVARS.keys():
                print(f"Plotting {FIGVARS[user_in]}.")
                interactive_plot(df, user_in, BOOMS, rawpath)
            elif user_in in NIFIGVARS.keys():
                normal_plot(df, user_in, BOOMS)
            elif user_in in {"quit", "exit", "qq"}:
                break
            elif user_in in {"help", "vars", "?"}:
                print(f"Available variables: {FIGVARS}")
            elif user_in in {"table", "df", "data"}:
                print(df)
            else:
                print(f"Unrecognized variable '{user_in}'.")
        except KeyboardInterrupt:
            break

def main():
    args = parse("interact")

    dirs = args["process"]
    selection = args["selection"]
    if selection not in dirs:
        raise KeyError(f"{selection} not in process keys")
    
    rawpath = os.path.join(args["data"], dirs[selection])
    procpath = os.path.join(results_dir, "testing" if args["test"] else "processed", f"{selection}.csv")

    df = pd.read_csv(procpath)
    df["time"] = pd.to_datetime(df["time"])
    df.set_index("time", inplace = True)

    interact_CLI(df, rawpath)


if __name__ == "__main__":
    main()
