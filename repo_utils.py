
from datetime import date
import os.path
import pandas as pd
import numpy as np
import math
from random import sample
import yaml
import os
import sys

# Constants: typical file names and directory paths

def get_dropbox_root(config_path: str = "user_config.yml") -> str:
    """Gets the Dropbox root folder in an OS- and user-independent way
    Args:
        config_path (str, optional): A full or relative path to the user_config
        containing (among other things) the Dropbox folder location on the local
        system. Defaults to "user_config.yml".
    Returns:
        str: The full path to the Dropbox root folder, formatted correctly for OS
    """
    if not os.path.isabs(config_path):
        config_path = os.path.join(os.path.dirname(__file__), config_path)
    with open(config_path, "r") as stream:
        data = yaml.safe_load(stream)
    return data["dropbox_root"]
    
def get_latest_path(path, date_length: int=6):
    """Function to get the latest path
    Args:
        path (str): full path to file to get latest
        date_lenght (int): length of date, defaults to 6
    Returns:
        str: string of latest path to the selected file
    """
    files = [x for x in os.listdir(os.path.dirname(path)) if os.path.basename(path)[date_length:] in x]
    files.sort(reverse=True)
    latest_file = files[0]
    latest_path = os.path.join(os.path.dirname(path), latest_file)
    return latest_path

dropbox_root = get_dropbox_root()

project_root = os.path.join(
    dropbox_root,
    "CB Data Analytics",
    "1 Projects",
    "12. KPLC Pilot Study",
)

"""
lab: utilities for working with lab data

The intention of this package is to encourage users to put all that they know
about the Odyssey data sources into a single text file: the Odyssey data 
dictionary. This will act as the "golden source of truth" on Odyssey's data. 
The code in _this_ package is designed to parse the data dictionary and to 
provide a higher-level syntax for getting to the useful data that we need.
"""

import yaml
import warnings
import pandas as pd
from typing import List, Optional, Any
import os

try:
    import pyodbc
except ImportError:
    warnings.warn(
        "The package pyodbc is not found. Not the end of the world, but you cannot use ODBC connectivity."
    )
try:
    import elasticsearch
except ImportError:
    warnings.warn(
        "The package elasticsearch is not found. Not the end of the world, but you cannot use RESTful web API connectivity."
    )


def load_metadata(
    data_dict_file: str = "lab_data_dict.yml",
    secrets_file: str = "odyssey_secrets.yml",
) -> dict:
    """Reads all metadata from key YAML files, to be reused throughout

    Args:
        data_dict_file (str, optional): The path to the main data dictionary,
        containing everything but the secrets. Defaults to "odyssey_data_dict.
        yml".
        secrets_file (str, optional): The path to the connection secrets,
        typically the username and password. Defaults to "odyssey_secrets.yml".

    Returns:
        dict: all of the metadata for the connection
    """
    if not os.path.isabs(secrets_file):
        secrets_file = os.path.join(os.path.dirname(__file__), secrets_file)
    with open(secrets_file) as file:
        secrets = yaml.safe_load(file)

    if not os.path.isabs(data_dict_file):
        data_dict_file = os.path.join(os.path.dirname(__file__), data_dict_file)
    with open(data_dict_file) as file:
        metadata = yaml.safe_load(file)

    metadata["secrets"] = secrets

    return metadata


def create_connection(type: str = "rest", metadata: Optional[dict] = None) -> Any:
    """Creates a connection to Odyssey of the selected type

    Args:
        type (str, optional): The API to be used, currently only "rest" or "odbc_dsn".
        Defaults to "rest".
        metadata (Optional[dict], optional): The Odyssey metadata, typically
        returned by `load_metadata()` and containing all of the credentials.
        Defaults to None, which loads the metadata from the default locations.

    Returns:
        Any: A connection-type object, as appropriate for the connection type
    """
    if metadata is None:
        metadata = load_metadata()

    if type == "odbc_dsn":
        connection_credentials = [
            x["kwargs"] for x in metadata["connections"] if x["type"] == "odbc_dsn"
        ][0]
        connection_credentials.update(metadata["secrets"])
        return pyodbc.connect(**connection_credentials)
    elif type == "rest":
        connection_credentials = [
            x["kwargs"] for x in metadata["connections"] if x["type"] == "rest"
        ][0]
        # Slightly different secrets syntax compared to ODBC, but same underlying creds
        connection_credentials.update(
            {"http_auth": (metadata["secrets"]["uid"], metadata["secrets"]["pwd"])}
        )
        return elasticsearch.Elasticsearch(**connection_credentials)
    else:
        raise ValueError(f"Connection type {type} not currently supported.")


def list_all_tables(cnxn: Optional[Any] = None) -> pd.DataFrame:
    """Lists all table names for the current connection

    Args:
        cnxn (Optional[Any], optional): An Odyssey connection, as typically
        returned by `create_connection()`. Defaults to None, which is the same
        as using the default connection in `create_connection()`.

    Returns:
        pd.DataFrame: A list of all the tables that the connection recognizes.
    """
    if cnxn is None:
        cnxn = create_connection()

    # Because we don't REQUIRE PyODBC or Elasticsearch as modules, the connection
    # checking is a bit awkward here:
    cnxn_str = str(type(cnxn))
    if cnxn_str == "<class 'elasticsearch.client.Elasticsearch'>":
        return sql_to_df("SHOW TABLES", cnxn)
    elif cnxn_str == "<class 'pyodbc.Connection'>":
        cursor = cnxn.cursor()
        return pd.DataFrame(
            columns=["name", "type"],
            data=[[x[2], x[3]] for x in cursor.tables()],
        )
    else:
        raise ValueError(f"Connection type {cnxn_str} not currently supported.")


def list_all_columns(table_name: str, cnxn: Optional[Any] = None) -> pd.DataFrame:
    """Lists all columns in the requested table. Due to oddities in the Elasticsearch
    API, some of these columns may not be SELECTable (bad column names seem to be the
    cause), and due to Odyssey conventions, MANY columns may not contain data or be at
    all useful.

    Args:
        table_name (str): The table's name
        cnxn (Optional[Any], optional): An Odyssey connection, as typically
        returned by `create_connection()`. Defaults to None, which is the same
        as using the default connection in `create_connection()`. Only the rest
        connection is currently supported for this function.

    Returns:
        pd.DataFrame: A table listing all of the column names ("column"), their SQL
        type ("type"), and a mapping to something Python-ish ("mapping")
    """
    if cnxn is None:
        cnxn = create_connection()

    # Because we don't REQUIRE PyODBC or Elasticsearch as modules, the connection
    # checking is a bit awkward here:
    cnxn_str = str(type(cnxn))
    if cnxn_str == "<class 'elasticsearch.client.Elasticsearch'>":
        df = sql_to_df(f"SHOW COLUMNS IN {table_name}", cnxn)
        # STRUCT columns show in this query, but they cannot be asked to return data.
        # Filter them out: their subfields are also listed and CAN be queried
        return df[df["type"] != "STRUCT"].reset_index(drop=True)
    else:
        raise ValueError(f"Connection type {cnxn_str} not currently supported.")


def get_useful_columns(
    table_name: str, format: Optional[str] = "str", metadata: Optional[dict] = None
) -> Any:
    """Provides the names of the useful columns for a particular table

    Args:
        table_name (str): The table's name
        format (Optional[str], optional): Either "str", "dataframe", or "list", makes
        the function return either a single, comma-separated string (useful for SQL
        queries) or a DataFrame (complete with descriptions of each column) or
        list of strings (useful for pandas slicing). Defaults to "str".
        metadata (Optional[dict], optional): The Odyssey metadata, typically
        returned by `load_metadata()` and containing all of the raw table metadata.
        Defaults to None, which loads the metadata from the default locations.

    Returns:
        Either a comma-separated string or a dataframe or a list of strings of column names that are worth loading and using.
    """
    if metadata is None:
        metadata = load_metadata()

    cols = [x["useful_columns"] for x in metadata["tables"] if x["name"] == table_name]

    if len(cols) == 0:
        raise ValueError(
            f"Table {table_name} not found in metadata. Check your spelling?"
        )
    else:
        cols = cols[0]

    if format == "dataframe":
        return pd.DataFrame(data=cols)
    elif format == "list":
        return [x["name"] for x in cols]
    elif format == "str":
        return ", ".join([x["name"] for x in cols])
    else:
        raise ValueError(f"Format type {format} not currently supported.")


def sql_to_df(
    sql_query: str, cnxn: Optional[Any] = None, show_progress: Optional[bool] = False
) -> pd.DataFrame:
    """Queries the Odyssey Database and returns the data to a Pandas DataFrame

    Args:
        sql_query (str): A valid SQL query. Multiline strings via triple quotes seem to be fine.
        cnxn (Optional[Any], optional): An Odyssey connection, as typically
        returned by `create_connection()`. Defaults to None, which is the same
        as using the default connection in `create_connection()`.
        show_progress (Optional[bool], optional): Prints progress on large queries, once every 10k rows. Defaults to False.

    Returns:
        pd.DataFrame: The Odyssey data requested by the SQL string
    """
    if cnxn is None:
        cnxn = create_connection()

    cnxn_str = str(type(cnxn))
    if cnxn_str == "<class 'elasticsearch.client.Elasticsearch'>":
        keep_going = True
        dfs = []
        iter_num = 1
        rows = None
        cols = None
        columns_meta = None
        cursor = None

        while keep_going:
            if iter_num == 1:
                kw = {
                    "self": cnxn,
                    "body": {"query": sql_query, "field_multi_value_leniency": "true"},
                }
            else:
                kw = {
                    "self": cnxn,
                    "body": {"cursor": cursor},
                }
            response = elasticsearch.client.SqlClient.query(**kw)

            if iter_num == 1:
                columns_meta = response["columns"]
                cols = pd.json_normalize(columns_meta)["name"]
                # Hack to clear useless name from DF index-- inelegant, but works:
                cols.name = None

            rows = response["rows"]
            df = pd.DataFrame(rows, columns=cols)
            # A bit messy to typecast dates here. I'd rather do it as the df is
            # constructed or all in a single go at the end; I hope that this compromise
            # conserves memory without killing performance:
            for col in columns_meta:
                if col["type"] == "datetime":
                    df[col["name"]] = pd.to_datetime(df[col["name"]])
                elif col["type"] == "integer":
                    df = df.astype({col["name"]: "Int32"})
            dfs += [df]

            # Check for a cursor in the response, indicating that more rows are to come.
            if "cursor" in response.keys():
                cursor = response["cursor"]
                keep_going = True
                if show_progress and (iter_num % 10 == 0):
                    print(f"Up to {iter_num}k rows read")
                iter_num += 1
            else:
                keep_going = False
                if show_progress:
                    print("All rows read")
                return pd.concat(dfs, ignore_index=True)
    elif cnxn_str == "<class 'pyodbc.Connection'>":
        if show_progress:
            print("Showing progress is not currently supported for ODBC connections")
        return pd.read_sql(sql_query, cnxn)
    else:
        raise ValueError(f"Connection type {cnxn_str} not currently supported.")


def check_for_primary_key(
    df: pd.DataFrame, candidate: List[str], show_debug: Optional[bool] = True
) -> bool:
    """Checks if a given subset of columns is a primary key for a DataFrame

    Args:
        df (pd.DataFrame): A data frame
        candidate (List[str]): A list of some subset of df's columns. The function will
        check if each row of df has a unique combination of the candidate values
        show_debug (Optional[bool], optional): Whether to print diagnostic details to
        the screen. Defaults to True.

    Returns:
        bool: Whether or not the candidate key is a primary key for df
    """
    dupes = df[df.duplicated(subset=candidate, keep=False)]
    if show_debug:
        print(
            f"There are {len(df)} rows in the DataFrame, and {len(dupes)} of them ({100*len(dupes)/len(df)}%) have duplicate candidate key values."
        )
        if len(dupes) == 0:
            print("The candidate key [" + ", ".join(candidate) + "] IS a primary key.")
        else:
            print(
                "The candidate key [" + ", ".join(candidate) + "] is NOT a primary key."
            )
    return len(dupes) == 0


def check_for_functional_dependency(
    df: pd.DataFrame,
    determinant: List[str],
    dependent: List[str],
    show_debug: Optional[bool] = True,
) -> bool:
    """Check if named dependent keys are, in fact, functionally dependent in a DataFrame

    Args:
        df (pd.DataFrame): The DataFrame of interest
        determinant (List[str]): The potential determinant key set
        dependent (List[str]): The potential dependent key set
        show_debug (Optional[bool], optional): Whether to print diagnostic details to
        the screen. Defaults to True.

    Returns:
        bool: Returns True only if _all_ of the candidate dependent keys show functional dependency on the determinant keys after being checked one-by-one
    """
    result = True
    all_keys = determinant + dependent
    if len(all_keys) != len(set(all_keys)):
        raise RuntimeError(
            "There can be no overlaps between the determinant and the dependent keys to be checked."
        )
    for dep_key in dependent:
        df_tmp = df[determinant + [dep_key]].drop_duplicates()
        df_determinant = df_tmp.drop_duplicates(subset=determinant)
        is_dependent = len(df_tmp) == len(df_determinant)
        result = result and is_dependent
        if show_debug:
            print(f"For key {dep_key}:")
            if is_dependent:
                print("This key IS dependent on the determinant.")
            else:
                print("This key is NOT functionally dependent.")
            print(
                f"There are {len(df_determinant)} unique values of the determinant key"
            )
            print(
                f"There are {len(df_tmp[[dep_key]].drop_duplicates())} unique values of the key {dep_key}."
            )
    return result

def parse_to_sql(list_to_parse, how: str = "str", function: str = ""):
    """Function to parse list to elastic search format
    Args:
        list_to_parse (list): List parsing to perform query
        how (str): list or string. List will give a list of strings
        that can be used to loop while str will put all values in 
        a single str
        function (str): type of summarizing function to be added to each variable in the list
        it can be any of SQL SUM, COUNT, DISTINCT
    Returns:
        str: string of parsed
    """
    if how == "list":
        parsed_list = "('" + "', '".join(list_to_parse) + "')"
    elif function == "":
        parsed_list = "', '".join(list_to_parse)
    elif function != "DISTINCT":
        parsed_list = " ".join([f"{function}({x})" for x in list_to_parse])
    else:
        parsed_list = " ".join([f"COUNT( DISTINCT {x})" for x in list_to_parse])
    return parsed_list

 
def query(
    sql_query: Optional[str] = "",
    table_name: Optional[str] = "",
    columns: Optional[list] = "",
    filters: Optional[str] = "",
    run_queries: Optional[bool] = True,
    show_progress: Optional[bool] = False
    ) -> pd.DataFrame:
    """Function to run a simplified version of Odyssey queries. It can also run regular SQL queries, perform some pre-formatting for dates
    and calculate other columns.
    Args:
        sql_query (str, optional): regular elasticsearch SQL query using fields available in Odyssey.
        table_name (str, optional): name of table including raw (hourly), daily, and monthly.
        columns (list, optional): list of columns using the Lab convention. Go to lab_data_dict.yml for details.
        filters (str, optional): a single string containing filters for the query. It's the "WHERE" part of an SQL query. You can use either Oydssey or Lab names.
        run_queries: (bool, optional): whether to run this query, or not. Preferrably link this to a variable in the beginning of your notebook that will
        deactivate all the queries at once. This way we can stop commenting out queries throughout a notebook to avoid running them. Defaults to True.
        show_progress (bool, optional) : whether you want to see progress of query. Defaults to False.
    """
    metadata = load_metadata()

    if run_queries == False:
        print("CAUTION: You are about to run the rest of the analysis using data previously downloaded. You are not runing a new query.")
        return
    if sql_query != "":
        df = sql_to_df(sql_query, show_progress=show_progress)
        tables = [x["name"] for x in metadata["tables"]]
        query_list = [x.strip() for x in sql_query.split(" ")]
        table_name = [x for x in query_list if x in tables]
        table = metadata["tables"]["name" == table_name]
        table = pd.json_normalize(table["useful_columns"])
        table_columns = {i:[j, k] for i, j, k in zip(table["name"], table["lab_name"], table["type"])}
        df.columns = [table_columns[x][0] if x in list(table["name"]) else x for x in df.columns]
    else:    
        #Checking that the values needed for the query are included
        if "ACPU" in columns and "consump_kwh" not in columns:
            columns.insert(len(columns), "consump_kwh")
        
        if "ACPU" or "ARPU" in columns and "meter_count" not in columns:
            columns.insert(len(columns), "meter_count")

        if "ARPU" in columns and "revenue_lc" not in columns:
            columns.insert(len(columns), "revenue_lc")

        if "year_month" in columns and "timestamp" not in columns:
            columns.insert(len(columns), "timestamp")

        metadata = load_metadata()

        table = [x for x in metadata["tables"] if x["lab_name"] == table_name]
        table, odyssey_table_name = pd.json_normalize(table[0]["useful_columns"]), table[0]["name"]
        table_columns = {i:[j, k] for i, j, k in zip(table["lab_name"], table["name"], table["type"])}

        # Categorizing fields based on required pre-processing/calculation
        sum_columns_pre_query, count_columns_pre_query, calculated_columns_post_query, group_by_cols= [
        [x for x in list(table_columns.keys()) 
        if table_columns[x][1] in l]
        for l in [["int", "float"],["count"],["calculated_field"],["int_date", "str", "datetime", "hour_int"]]]

        # Assign input fields into each of the previous categories
        sum_columns_pre_query, count_columns_pre_query, calculated_columns_post_query, group_by_cols = [[x for x in category if x in columns] for category in [sum_columns_pre_query, count_columns_pre_query, calculated_columns_post_query, group_by_cols]]

        # Getting the Oddysey names of each field
        calc_sum_pre_ody, calc_count_pre_ody, group_by_cols_ody = [[table_columns[x][0] for x in l] for l in [sum_columns_pre_query, count_columns_pre_query, group_by_cols]]

        # Parsing for SQL based on each type of manipulation
        group_by_cols_sql = ", ".join([f"{x} {y}" for x,y in zip(group_by_cols_ody, group_by_cols)])

        if len(calc_sum_pre_ody + sum_columns_pre_query) > 0:
            sum_cols_sql = ", "  +  ", ".join([f"SUM({x}) {y}" for x,y in zip(calc_sum_pre_ody, sum_columns_pre_query)])
        else:
            sum_cols_sql = ""
        if len(calc_count_pre_ody + count_columns_pre_query) > 0:
            count_cols_sql = ", " +  ", ".join([f"{x} {y}" for x,y in zip(calc_count_pre_ody, count_columns_pre_query)])
        else:
            count_cols_sql = ""
        
        filters_sql =       " ".join([table_columns[x] if x in list(table["lab_name"][0]) else x for x in filters.split()])

        query = f"""SELECT {group_by_cols_sql} {sum_cols_sql} {count_cols_sql}
                    FROM {odyssey_table_name}
                    WHERE {filters_sql}
                    GROUP BY {" ,".join(group_by_cols)}
            """

        df = sql_to_df(query, show_progress=show_progress)

        if "ACPU" in columns:
            df["ACPU"] = df["consump_kwh"] / df["meter_count"]

        if "ARPU" in columns:
            df["ARPU"] = df["revenue_lc"] / df["meter_count"]

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc = True)

        if "year_month" in columns:
            df["year_month"] = df["timestamp"].dt.tz_localize(None).dt.to_period('M')
    
    return df

def get_year_month_col(df, date_col = "timestamp"):
    df["year_month"] = df[date_col].dt.tz_localize(None).dt.to_period('M')


""" Plotly templates, graph and text color palettes
    The template can be set by running the following 
    There are two different palettes:
        - 'text_palette' includes the five main CB-style colors for text
        - 'graphs_palette' color scales used for graphs (blue, gray, red, green)
"""

import plotly.graph_objs as go
import plotly.io as pio
from sys import platform as _platform

graphs_palette = {
    "blue_1": "rgb(52, 58, 100)",
    "blue_2": "rgb(51, 58, 135)",
    "blue_3": "rgb(157, 177, 208)",
    "blue_4": "rgb(195, 207, 226)",
    "gray_1": "rgb(0, 0, 0)",
    "gray_2": "rgb(128, 128, 128)",
    "gray_3": "rgb(150, 150, 150)",
    "gray_4": "rgb(192, 192, 192)",
    "red_1": "rgb(232, 28, 0)",
    "red_2": "rgb(195, 10, 61)",
    "red_3": "rgb(250, 134, 148)",
    "red_4": "rgb(255, 193, 193)",
    "green_1": "rgb(152, 219, 212)",
    "green_2": "rgb(176, 235, 229)",
    "green_3": "rgb(205, 254, 243)",
    "green_4": "rgb(247, 255, 247)",
}

text_palette = {
    "header_blue": "rgb(52,58,100)",
    "gray_main": "rgb(105,105,105)",
    "light_gray": "rgb(191,191,191)",
    "yellow": "rgb(255,192,0)",
    "magenta": "rgb(196,40,90)",
}


if _platform == "darwin":
    text_font = "Gill Sans"
else:
    text_font = "Gill Sans MT"


def use_template():
    """Sets CB style template for the for the notebook."""
    fig = go.Figure(
        layout=dict(
            title="Figure Title",
            font=dict(size=14, family=text_font, color=text_palette["gray_main"]),
            showlegend=True,
            plot_bgcolor="white",
            colorway=[
                graphs_palette[x] for x in ["blue_1", "gray_1", "red_1", "green_1"]
            ],  # Can we do better?
        )
    )

    fig.update_xaxes(
        showline=True, linewidth=1, linecolor=text_palette["gray_main"], title="X Axis"
    )
    fig.update_yaxes(
        showline=True,
        linewidth=1,
        linecolor=text_palette["gray_main"],
        title="Y Axis",
        showticklabels=True,
        ticks="outside",
        tickwidth=1,
        tickcolor=text_palette["gray_main"],
    )
    templated_fig = pio.to_templated(fig)

    pio.templates["cb_graph"] = templated_fig.layout.template

    pio.templates.default = "cb_graph"


# Execute when imported: this registers the above code as the default template
use_template()

