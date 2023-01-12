# ETROC Analysis Scripts

Set of scripts to analyse data from the ETROC, whichever the source.
A set of systems have been tested with these scripts, in particular:
* ETROC1 Telescope in test beam with 3 ETROC1 boards
* ETROC1 Telescope in the CMS test area in Wilson Hall 14th floor taking cosmics (Work in Progress)
* Single ETROC1 board in laser setup (To be tested)
* ETROC1 Telescope with ETROC2 Emulator (Future plan)

## Installation (First setup)

Create a venv to use stuff:
`python -m venv venv`

Activate the venv:
`source venv/bin/activate`

Then install dependencies:
```
python -m pip install lip-pps-run-manager
python -m pip install plotly
python -m pip install pandas
python -m pip install pyarrow
python -m pip install statsmodels
```

## How to use

At the start of a session, make sure the the venv is activated.
If not, just run the command:
`source venv/bin/activate`

Then run the desired script:
`python [script] [options]`

The options are different from script to script.
The scripts use argparse for defining the options, so use the `-h` flag to get help and list all the options.

### End of session

Once you are done, deactivate the venv with:
`deactivate`
