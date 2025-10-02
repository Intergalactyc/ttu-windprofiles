from windprofiles import Location
import os

OUTLIER_REMOVAL_WINDOW = 60*50*5 # records; this is 5 minutes
OUTLIER_REMOVAL_SIGMA = 5

NPROC = max(os.cpu_count() - 1, 1)

LOCATION = Location(latitude=33.59, longitude=-102.03, elevation=1014., timezone="US/Central")

SOURCE_TIMEZONE = "UTC"

SOURCE_UNITS = {
    "p" : "inHg",
    "t" : "F",
    "ts" : "F",
    "rh" : "%",
    "u" : "mph",
    "v" : "mph",
    "w" : "mph",
    "ws" : "mph",
    "wd" : ("degrees", "N", "CW")
}

SOURCE_UNITS = {
    "p" : "inHg",
    "t" : "F",
    "ts" : "F",
    "rh" : "%",
    "u" : "mph",
    "v" : "mph",
    "w" : "mph",
    "ws" : "mph",
    "wd" : ("degrees", "N", "CW")
}

FIGVARS = {
    "p" : "Barometric pressure",
    "t" : "Temperature",
    "ts" : "Sonic temperature",
    "vpt" : "Virtual potential temperature",
    "ws" : "Wind speed",
    "u" : "U (North) wind component",
    "v" : "V (West) wind component",
    "w" : "W (Vertical) wind component",
    "rh" : "Relative humidity",
    "w'u'" : "Vertical flux of u-momentum",
    "w'v'" : "Vertical flux of v-momentum",
    "w'vpt'" : "Vertical heat flux",
}

NIFIGVARS = {
    "alpha" : "Wind shear exponent",
    "ti" : "Turbulence intensity",
    "ri_bulk" : "Bulk Richardson number"
}

FIGUNITS = {
    "p" : "kPa",
    "t" : "K",
    "ts" : "K",
    "vpt" : "K",
    "ws" : "m/s",
    "u" : "m/s",
    "v" : "m/s",
    "w" : "m/s",
    "rh" : "decimal fraction",
    "w'u'" : "m^2/s^2",
    "w'v'" : "m^2/s^2",
    "w'vpt'" : "K*m/s"
}

SOURCE_HEADERS = ["TSU_1", "TSV_1", "TSW_1", "TST_1", "TT_1", "TRH_1", "TBP_1", "TSU_2", "TSV_2", "TSW_2", "TST_2", "TT_2", "TRH_2", "TBP_2", "TSU_3", "TSV_3", "TSW_3", "TST_3", "TT_3", "TRH_3", "TBP_3", "TSU_4", "TSV_4", "TSW_4", "TST_4", "TT_4", "TRH_4", "TBP_4", "TSU_5", "TSV_5", "TSW_5", "TST_5", "TT_5", "TRH_5", "TBP_5", "TSU_6", "TSV_6", "TSW_6", "TST_6", "TT_6", "TRH_6", "TBP_6", "TSU_7", "TSV_7", "TSW_7", "TST_7", "TT_7", "TRH_7", "TBP_7", "TSU_8", "TSV_8", "TSW_8", "TST_8", "TT_8", "TRH_8", "TBP_8", "TSU_9", "TSV_9", "TSW_9", "TST_9", "TT_9", "TRH_9", "TBP_9", "TSU_10", "TSV_10", "TSW_10", "TST_10", "TT_10", "TRH_10", "TBP_10", "TSN-TRANS_1", "TSW-TRANS_1", "TSV-TRANS_1", "TS-WS_1", "TS-WD_1", "TSN-TRANS_2", "TSW-TRANS_2", "TSV-TRANS_2", "TS-WS_2", "TS-WD_2", "TSN-TRANS_3", "TSW-TRANS_3", "TSV-TRANS_3", "TS-WS_3", "TS-WD_3", "TSN-TRANS_4", "TSW-TRANS_4", "TSV-TRANS_4", "TS-WS_4", "TS-WD_4", "TSN-TRANS_5", "TSW-TRANS_5", "TSV-TRANS_5", "TS-WS_5", "TS-WD_5", "TSN-TRANS_6", "TSW-TRANS_6", "TSV-TRANS_6", "TS-WS_6", "TS-WD_6", "TSN-TRANS_7", "TSW-TRANS_7", "TSV-TRANS_7", "TS-WS_7", "TS-WD_7", "TSN-TRANS_8", "TSW-TRANS_8", "TSV-TRANS_8", "TS-WS_8", "TS-WD_8", "TSN-TRANS_9", "TSW-TRANS_9", "TSV-TRANS_9", "TS-WS_9", "TS-WD_9", "TSN-TRANS_10", "TSW-TRANS_10", "TSV-TRANS_10", "TS-WS_10", "TS-WD_10"]

HEADER_MAP = {
    "TSU" : None,
    "TSV" : None,
    "TSW" : None,
    "TST" : "ts",
    "TT" : "t",
    "TRH" : "rh",
    "TBP" : "p",
    "TSN-TRANS" : "u",
    "TSW-TRANS" : "v",
    "TSV-TRANS" : "w",
    "TS-WS" : "ws",
    "TS-WD" : "wd"
}

HEADER_MAP_INV = {v : k for k, v in HEADER_MAP.items()}

BOOMS_LIST = list(range(1, 11))
HEIGHTS_LIST = [0.9, 2.4, 4.0, 10.1, 16.8, 47.3, 74.7, 116.5, 158.2, 200.0]
HEIGHTS = {b : h for b, h in zip(BOOMS_LIST, HEIGHTS_LIST)}
DROP_BOOMS = [8, 10]
