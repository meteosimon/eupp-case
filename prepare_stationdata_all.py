#!/usr/bin/env python3
# -------------------------------------------------------------------
# Downloader for the EUPP Hacky Benchmark Station Dataset
#
# Also works on the European Weather Cloud for data which is not
# publicly available (Switzerland).
#
# Developed on Python version 3.10.4
#
# Authors: Thorsten Simon and Reto Stauffer
# Date: 2022-09-16
# -------------------------------------------------------------------

from functions import *

from prepare_stationdata import main


# -------------------------------------------------------------------
# Main part of the Script
# -------------------------------------------------------------------
if __name__ == "__main__":

    # Some settings
    CSVDIR = "csvdata"

    # Start downloading
    for c in ["germany", "france", "netherlands", "switzerland", "austria"]:
        main({"prefix": "euppens", "country": c.lower(), "param": "t2m", "nocache": False})

    print("\n .... everything done.")


