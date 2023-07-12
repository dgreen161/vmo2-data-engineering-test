## VMO2 Data Engineering Tech Test

### Initialising

These steps are required to clone the repository and set up the virtual environment within which the pipeline can be run. The code was written in Python 3.9.6, make sure you have the same version.

1. Clone the repository: `git clone <repo>`
1. Move into the new repository: `cd vmo2-data-engineering-test`
1. Create a virtual environment: `python3 -m venv env`
1. Activate the virtual environment: `source env/bin/activate`
1. Install the dependencies: `pip install -r requirements.txt`

### Running the pipeline

To run the pipeline use the following command: 
```shell
python -m tasks
```

Within the pipeline, the following transformations are applied:
1. Selects required columns from the input CSV, formatting the datatypes of the columns
1. Filters transaction_amount greater than 20
1. Exclude transactions made before 2010
1. Converts to date to string and returns a tuple
1. Sums the transaction_amount by date
1. Converts to JSON in preparation for writing to file

### Run the unit tests

To run the unit tests use the following command: 
```shell
python -m unittest
```

You should expect to see 1 test run returning `OK` on completion.
