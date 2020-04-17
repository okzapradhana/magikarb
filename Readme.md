
# Magikarb

Using Python 3.6.9.
 
 ## Install
 To install all packages. Create `virtual environment` first then run this command on your directory:
 `pip install -r requirement.txt`
Before I choose what file type to generate, I did comparation by benchmark some file type including:
- csv
- sqlite (.db)
- json
- msgpack
- pkl
- tinydb (.json)
At the end I use `.csv` files because it has smallest file size and fastest writing time. I prove that by create `benchmark_file.py` files inside this repo.

## Usage
To <b>generate</b> file, you may run `solution.py` with some kind of example as below:
`python solution.py --output="output/efishery.csv"`
If you want to generate using `tinydb` or `sqlite` . You may use this command:
`python solution.py --output="output/efishery.csv" --db-type="sqlite"` or replace `sqlite` with `tinydb`

To <b>query</b> file, you may run `query.py` with some kind of example as below:
`python query.py --head=100` to show first 100 rows.
`python query.py --tail=100` to show last 100 rows.
`python query.py --head=100 --filter-data="username"` to show first 100 rows of username column
`python query.py --row=101` to show data at row 101.
`python query.py --agg="username" --agg="count"` to grouping data then aggregate to get the `count`

## Data Structure
I used several data structures which will explained as below:

- device_id: `list`
- username: `list`
- lokasi: `list`
- lokasi relation with username: `dict`
- device relation with username: `dict`
- amount: `list`
- start_date and end_date: `float`