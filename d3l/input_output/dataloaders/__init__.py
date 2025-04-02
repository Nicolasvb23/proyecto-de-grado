"""
Copyright (C) 2021 Alex Bogatu.
This file is part of the D3L Data Discovery Framework.
Notes
-----
This module exposes data reading functionality.
"""

from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Tuple, Union, Dict, Any

import pandas as pd
import os


class DataLoader(ABC):
    @abstractmethod
    def get_counts(
        self,
        table_name: str,
    ) -> Dict[str, Tuple[int, int]]:
        """
        Reads the non-null and distinct cardinality of each column of a table.

        Parameters
        ----------
        table_name : str
            The table name.

        Returns
        -------
        Dict[str, Tuple[int, int]]
            A collection of tuples of non-null and distinct cardinalities.

        """
        pass

    @abstractmethod
    def get_columns(
        self,
        table_name: str,
    ) -> List[str]:
        """
        Retrieve the column names of the given table.

        Parameters
        ----------
        table_name : str
            The table name.

        Returns
        -------
        List[str]
            A collection of column names as strings.

        """
        pass

    @abstractmethod
    def get_tables(
        self,
    ) -> List[str]:
        """
        Retrieve all the table names existing under the given root.

        Parameters
        ----------
        root_name : str
            The name of the schema if needed for database loaders.

        Returns
        -------
        List[str]
            A list of table names.
        """
        pass

    @abstractmethod
    def read_table(
        self,
        table_name: str,
        table_columns: Optional[List[str]] = None,
        chunk_size: Optional[int] = None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """
        Read the table data into a pandas DataFrame.

        Parameters
        ----------
        table_name : str
            The table name.
        table_columns : Optional[List[str]]
            A list of columns to be read.
        chunk_size : int
            The number of rows to read at one time.
            If None then the full table is returned.

        Returns
        -------
        Union[pd.DataFrame, Iterator[pd.DataFrame]]
            The entire table data or a Dataframe with *chunksize* rows.

        """
        pass


class CSVDataLoader(DataLoader):
    def __init__(self, root_path: str, **loading_kwargs: Any):
        """
        Create a new CSV file loader instance.
        Parameters
        ----------
        root_path : str
            A locally existing directory where all CSV files can be found.
        loading_kwargs : Any
            Pandas-specific CSV reading arguments.
            Note that all CSV file in the given root directory are expected to have the same formatting details,
            e.g., separator, encoding, etc.
        """

        if not os.path.isdir(root_path):
            raise FileNotFoundError(
                "The {} root directory was not found locally. "
                "A CSV loader must have an existing directory associated!".format(
                    root_path
                )
            )

        self.root_path = root_path
        if self.root_path[-1] != "/":
            self.root_path = self.root_path + "/"
        self.loading_kwargs = loading_kwargs

    def get_counts(self, table_name: str) -> Dict[str, Tuple[int, int]]:
        """
        Reads the non-null and distinct cardinality of each column of a table.

        Parameters
        ----------
        table_name : str
            The table (i.e, file) name without the parent directory path and *without* the CSV extension.

        Returns
        -------
        Dict[str, Tuple[int, int]]
            A collection of tuples of non-null and distinct cardinalities.

        """

        file_path = self.root_path + table_name + ".csv"
        print(file_path)
        data_df = pd.read_csv(file_path, **self.loading_kwargs)
        column_counts = {
            str(col): (data_df[col].count(), data_df[col].nunique())
            for col in data_df.columns
        }
        return column_counts

    def get_columns(
        self,
        table_name: str,
    ) -> List[str]:
        """
        Retrieve the column names of the given table.

        Parameters
        ----------
        table_name : str
            The table (i.e, file) name without the parent directory path and *without* the CSV extension.

        Returns
        -------
        List[str]
            A collection of column names as strings.

        """
        file_path = self.root_path + table_name + ".csv"
        data_df = pd.read_csv(file_path, nrows=1, **self.loading_kwargs)
        return data_df.columns.tolist()

    def get_tables(self) -> List[str]:
        """
        Retrieve all the table names existing under the given root.

        Parameters
        ----------
        None

        Returns
        -------
        List[str]
            A list of table (i.e., file) names that *do not* include full paths or file extensions.

        """
        result_of_tables = [
            ".".join(f.split(".")[:-1])
            for f in os.listdir(self.root_path)
            if str(f)[-4:].lower() == ".csv"
        ]
        return result_of_tables

    def read_table(
        self,
        table_name: str,
        table_columns: Optional[List[str]] = None,
        chunk_size: Optional[int] = None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """
        Read the table data into a pandas DataFrame.

        Parameters
        ----------
        table_name : str
            The table (i.e, file) name without the parent directory path and *without* the CSV extension.
        table_columns : Optional[List[str]]
            A list of columns to be read.
        chunk_size : int
            The number of rows to read at one time.
            If None then the full table is returned.

        Returns
        -------
        Union[pd.DataFrame, Iterator[pd.DataFrame]]
            The entire table data or a Dataframe with *chunksize* rows.

        """

        file_path = self.root_path + table_name + ".csv"
        if table_columns is not None:
            return pd.read_csv(
                file_path,
                usecols=table_columns,
                chunksize=chunk_size,
                low_memory=False,
                # error_bad_lines=False, # Deprecated in future versions
                # warn_bad_lines=False, # Deprecated in future versions
                **self.loading_kwargs,
            )
        return pd.read_csv(
            file_path,
            chunksize=chunk_size,
            low_memory=False,
            # error_bad_lines=False, # Deprecated in future versions
            # warn_bad_lines=False, # Deprecated in future versions
            **self.loading_kwargs,
        )

    def print_table_statistics(self) -> None:
        """
        Print the number of columns, rows, and total cells for each CSV in the directory.

        Returns
        -------
        None
        """
        num_rows = 0
        num_columns = 0
        total_cells = 0

        tables = self.get_tables()  # Get all table (CSV) names
        for table_name in tables:
            file_path = self.root_path + table_name + ".csv"

            # Load the CSV data (only necessary rows for counting)
            data_df = pd.read_csv(file_path, **self.loading_kwargs)

            # Get the number of rows, columns, and total cells
            num_rows += data_df.shape[0]
            num_columns += data_df.shape[1]
            total_cells += data_df.shape[0] * data_df.shape[1]

        print(f"Number of columns: {num_columns}")
        print(f"Number of rows: {num_rows}")
        print(f"Total number of cells: {total_cells}\n")
