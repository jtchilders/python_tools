#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$DIR/get_logtars.py -g "output.*.log"
$DIR/parse_pilotlogs.py -g "output.*.log"
$DIR/parse_athenaMPlogs.py -g "tarball_*"
source ~/scripts/setupROOT.sh
$DIR/plot_data.py -a athenadata.json -p pilotdata.json

