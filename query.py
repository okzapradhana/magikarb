import pandas as pd
import click

@click.command()
@click.option("--file-path", "-f", default="output/efishery.csv",
            help="Path to file to be processed.")
@click.option("--head", "-h", default=None, 
            help="Define amount of first rows to be showed.")
@click.option("--tail", "-t", default=None, 
            help="Define amount of last rows to be showed.")
@click.option("--filter-data", '-f', default=None,
            help="Filter based on column.")
@click.option("--row", '-r', default=None,
            help="Show data on specific row")
@click.option("--agg", '-a', multiple=True, default=None,
            help="Show aggregated value after grouping data")
def query(file_path, head, tail, filter_data, row, agg):
    df = pd.read_csv(file_path)
    print(head, tail, agg)
    if head is not None and tail is not None:
        df = show_first_last(df, head, tail)
        df = filter_col(df, head, tail, filter_data)
    #row starts from 0 (index) thus we decrease with 1
    if row is not None:
        df = df.iloc[row-1]
    if len(agg) > 0:
        df = df.groupby(agg[0]).agg(agg[1])
    
    return df    

def show_first_last(df, head, tail):
    if head is not None:
        df = df.head(head)
    else:
        df = df.tail(tail)
    
    return df

def filter_col(df, head, tail, filter_data):
    #filter col
    if filter_data is not None:
        if head is not None:
            df = df[[filter_data[0]]].head(head)
        else:
            df = df[[filter_data[0]]].tail(tail)

    return df

if __name__ == '__main__':
    result = query()
    print("Query Result:\n", result)
    