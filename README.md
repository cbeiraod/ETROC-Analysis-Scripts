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

### Running the Scripts

The scripts to run and the order to run them in depends on how the data is taken and the type of analysis which is desired to be performed. As a general rule of thumb, or as a starting point, the scripts should be run in the order presented below.

Typically, in a first step the data must be ingested, which is performed with one of the scripts: `process_etroc1_single_run_txt.py` or `process_etroc1_single_charge_injection_run.py`.
In a next step, cuts must be applied to the events with the `cut_etroc1_single_run.py` script.
Finally, the time resolution may be calculated using the `analyse_time_resolution.py` script.

In an alternative approach, it may be desirable to process multiple runs at once, for instance when processing the charge injection data, where there is a single run for each charge threshold pair, but the user is interested in the data from the aggregation of all the runs. In this approach the `process_etroc1_charge_injection_data_dir.py` script should be used, followed by the `analyse_dac_vs_charge.py` script. Since the `process_etroc1_charge_injection_data_dir.py` script automatically selects a default cut, it may be desired to customize cuts or other options, in this case the customizations should be performed after running the script and then the `reprocess_etroc1_charge_injection_data_dir.py` script should be called.

#### Script Details

The `process_etroc1_single_charge_injection_run.py` script reads a dat file (currently a single split of the file, but there is a TODO to support multiple splits). It optionally filters to only keep hit data and may add some additional metadata from the file name. Finally it saves all the data into an output SQLite table for later use.

The `process_etroc1_single_run_txt.py` script reads a txt file, produced as a summary from taking data with beam. It optionally filters to only keep hit data and may add some additional metadata from the file name. It also performs the task of matching board hits to build events. Finally it saves all the data into an output SQLite table for later use.

The `cut_etroc1_single_run.py` script ....

The `analyse_time_resolution.py` script ...

The `process_etroc1_charge_injection_data_dir.py` script automatically processes all the individual runs from the input data file by calling the `process_etroc1_single_charge_injection_run.py`, then creating a default cut file followed by calling the `cut_etroc1_single_run.py`script it finally merges all the summary data into a single dataset.

The `analyse_dac_vs_charge.py` script ...

The `reprocess_etroc1_charge_injection_data_dir.py` script effectively performs the same actions as the `process_etroc1_charge_injection_data_dir.py`, however it assumes the `process_etroc1_charge_injection_data_dir.py` script has been ran before. This reprocess script effectively allows to set new processing options as well as defining new cuts files for the individual runs.

### End of session

Once you are done, deactivate the venv with:
`deactivate`
