#helpers_2.py
# Funciones utilizadas pero que se descartan para evitar complejidad de dependencias, rendimiento y xq se usará Spark =)

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
import random
import numpy as np
from pyspark.sql import SparkSession
import time
import os
from decorators import *


## Función extraída de repositorio /Workspace/Repos/rodrigoasencios@bcp.com.pe/fabpyme/auxiliary_functions.py
def spark_to_pandas(df, batch_size = 50, sort_by = None, verbose = False):
    """
    Developer: Bruno Miranda - brunomiranda@bcp.com.pe
    """
    # retrieve spark cluster
    spark = SparkSession.builder.getOrCreate()
    dbutils = get_dbutils(spark)

    # generate random name
    random_name = str("%032x" % random.getrandbits(128))

    # create temp dataframe with identifier
    table_name = f'spark_to_pandas_{random_name}'
    table_full_name = f'catalog_lhcl_prod.bcp.bcp_edv_fabseg.{table_name}'
    join_key = 'index_key'

    # create index to use it as key when joining columns by batches
    df = df.select("*",
                   F.monotonically_increasing_id().alias(join_key))

    write_to_storage(df = df,
                     tableName = table_name,
                     npart = 20,
                     squad = 'fabrica',
                     location = None,
                     verbose = False)
    
    try:
        # read columns
        columns = spark.table(table_full_name).columns

        # remove join col otherwise there'd be duplicates when merging
        columns.remove(join_key)

        # create batch settings
        n_batch = np.ceil(len(columns) / batch_size).astype(int)

        # read only index and sort it
        pddf = spark.table_sql(table_name = table_full_name,
                               columns = join_key).toPandas()

        pddf.sort_values(by = join_key, ascending = True, ignore_index = True)

        for n_step in range(n_batch):
            start_idx = int(n_step * batch_size)
            end_idx = int((n_step + 1) * batch_size)
            if verbose: print(f'{bcolors.BOLD_RED}Batch {n_step + 1} [{start_idx} : {end_idx}] ({n_step + 1} / {n_batch}){bcolors.ENDC}')
            current_columns = [join_key] + columns[start_idx: end_idx]
            # read dataframe
            current_df = spark.table_sql(table_name = table_full_name,
                                         columns = current_columns)
            current_pddf = current_df.toPandas()

            # store in pandas dataframe
            pddf = pddf.merge(current_pddf, on = [join_key], how = 'left')

            # clean memory
            del current_pddf

        # remove join key
        pddf.drop(columns = join_key, inplace = True)
    
        # sort if required
        try:
            if sort_by is not None:
                if isinstance(sort_by, str):
                    sort_by = [sort_by]
                if verbose: print(f'{bcolors.BOLD_OKCYAN}\tSorted by {sort_by}{bcolors.ENDC}')
                pddf.sort_values(by = sort_by,
                                 ascending = [True for _ in sort_by],
                                 ignore_index = True,
                                 inplace = True)
        except:
            print('Could not be sorted, returning pandas dataset')
            return pddf
    except Exception as e:
        print('Could not convert it into pandas')
        print(e)
        drop_table(table_full_name)
        return None
    return pddf



## Función extraída de repositorio /Workspace/Repos/rodrigoasencios@bcp.com.pe/fabpyme/write_to_storage.py
def pd_write_in_chunks(df, directory_path, chunk_size = 10_000, chunk_name = 'chunk_id', file_format = 'parquet',
                       verbose = False, partitioned_by = None, schema = None, **kwargs):
    """
    Author: Bruno Miranda - brunomiranda@bcp.com.pe

    Description:
        - Similar to write_to_storage
    """
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"""Directory {directory_path} does not exist.""")
    else:
        # validate type of data
        dir_files = [f'{directory_path}/{dir_file}' for dir_file in os.listdir(directory_path)]
        flg_empty = len(dir_files) == 0
        flg_directory = all([os.path.isdir(dir_file) for dir_file in dir_files])

    # select write function
    try:
        write_func = LOC_WTS_PANDAS_WRITE_DIC[file_format]
    except KeyError:
        raise KeyError(f"""File format {file_format} not supported, only "parquet" and "csv" are supported.""")

    if partitioned_by is None:
        # validate type of content
        if flg_empty:
            pass
        elif flg_directory:
            partition_folder = dir_files[0].split('/')[-1].split('=')[0]
            print(f"{bcolors.BOLD_WARNING}Directory {directory_path} already exists and contains partitioned data: {partition_folder}. Removing it ...{bcolors.ENDC}")
            shutil.rmtree(directory_path)
            os.makedirs(directory_path, exist_ok = True)
        else:
            print(f"{bcolors.BOLD_WARNING}Removing partitioned data from {directory_path} ...{bcolors.ENDC}")
            shutil.rmtree(directory_path)
            os.makedirs(directory_path, exist_ok = True)

        # calculate number of chunks
        chunks = list(range(0, len(df), chunk_size))
        if verbose: n_chunks = len(chunks)
        # write each chunk
        for i, chunk in enumerate(chunks):
            if verbose: print(f'{bcolors.BOLD_RED}Chunk: {chunk:,} to {min(chunk + chunk_size, len(df)):,} ({i + 1} / {n_chunks}){bcolors.ENDC}')
            idx = str(i).zfill(6)
            # write chunk
            data = df.iloc[chunk:chunk + chunk_size]
            if schema is not None:
                data = data.astype(schema)
            write_func(data)(f'{directory_path}/{chunk_name}_{idx}.{file_format}', **kwargs)
    else:
        # validate if partitioned_by is a list
        if isinstance(partitioned_by, list):
            partitioned_by = partitioned_by[0]

        # validate if it belongs to pandas dataframe
        if partitioned_by not in df.columns.tolist():
            raise ValueError(f"""Partitioned column {partitioned_by} is not a column in the pandas dataframe.""")

        # validate type of content
        list_data_values = sorted(df[partitioned_by].unique().astype(int).tolist())
        n_list_data_values = len(list_data_values)

        if flg_empty:
            pass
        elif not flg_directory:
            raise ValueError(f"""Directory {directory_path} already exists and contains non partitioned data.""")
        else:
            # read current partitions
            current_partitions = sorted(os.listdir(directory_path))
            partition_field = current_partitions[0].split('=')[0]
            partition_values = sorted([current_partition.split('=')[-1].split('.')[0] for current_partition in current_partitions])

            # validate if partition is the same
            if partition_field != partitioned_by:
                raise ValueError(f"""Partitioned column {partitioned_by} is not the same as the one in the directory {directory_path}.""")

            list_to_replace = [value for value in list_data_values if float(str(value)) if int(str(value)) in map(lambda x: int(str(x)), partition_values)]
            list_to_add = [value for value in list_data_values if value not in list_to_replace]

            if len(list_to_replace)>0:
                print(f"{bcolors.WARNING}Replacing/Overwriting partitions: {', '.join(map(str, list_to_replace))}{bcolors.ENDC}")
            if len(list_to_add)>0:
                print(f"{bcolors.WARNING}Adding partitions: {', '.join(map(str, list_to_add))}{bcolors.ENDC}")

        # iterate over unique values
        for j, unique_value in enumerate(list_data_values):
            if verbose: print(f'{bcolors.BOLD_BLUE}{partitioned_by} = {unique_value} ({j + 1} / {n_list_data_values}){bcolors.ENDC}')
            current_path = f'{directory_path}/{partitioned_by}={unique_value}'
            # validate if path exists
            if not os.path.exists(current_path):
                print(f'{bcolors.BOLD_OKCYAN}Adding partition {unique_value} ...{bcolors.ENDC}')
                os.makedirs(current_path, exist_ok = True)
            else:
                print(f'{bcolors.BOLD_WARNING}Replacing partition {unique_value} ...{bcolors.ENDC}')
                shutil.rmtree(current_path)
                os.makedirs(current_path, exist_ok = True)

            # filter dataframe
            data = df[df[partitioned_by] == unique_value]
            chunks = list(range(0, len(data), chunk_size))
            if verbose: n_chunks = len(chunks)
            for i, chunk in enumerate(chunks):
                if verbose: print(f'{bcolors.BOLD_RED}\tChunk: {chunk:,} to {min(chunk + chunk_size, len(data)):,} ({i + 1} / {n_chunks}){bcolors.ENDC}')
                idx = str(i).zfill(6)

                # write chunk
                if schema is not None:
                    data = data.astype(schema)

                write_func(data.iloc[chunk:chunk + chunk_size])(f'{current_path}/{chunk_name}_{idx}.{file_format}', **kwargs)

                # clean memory
                del data
    if verbose: print(f'{bcolors.BOLD_OKCYAN}Done!{bcolors.ENDC}')



## Función extraída de repositorio /Workspace/Repos/rodrigoasencios@bcp.com.pe/fabpyme/write_to_storage.py
def pd_read_chunks(directory_path, file_format = 'parquet', verbose = False, **kwargs):
    # validate directory path
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f'Directory {directory_path} not found.')

    # select read function
    try:
        read_func = LOC_WTS_PANDAS_READ_DIC[file_format]
    except KeyError:
        raise KeyError(f"""File format {file_format} not supported, only "parquet" and "csv" are supported.""")

    # Declare empty daframe to save chunks
    pddf = pd.DataFrame()

    # get all chunks
    dir_files = sorted([f'{directory_path}/{dir_file}' for dir_file in os.listdir(directory_path)])

    if all([os.path.isdir(dir_file) for dir_file in dir_files]):
        temp = list()
        for dir_file in dir_files:
            temp.extend(sorted([f'{dir_file}/{d_f}' for d_f in os.listdir(dir_file)]))
        dir_files = temp
    else:
        raise FileNotFoundError('File do not match the expected pattern')

    # calculate number of files
    if verbose: n_files = len(dir_files)

    # load files from directory
    for i, dir_file in enumerate(dir_files):
        if verbose: print(f'{bcolors.BOLD_MAGENTA}{file_format} file {i + 1} / {n_files}{bcolors.ENDC}')
        pddf = pd.concat([pddf, read_func(dir_file, **kwargs)], axis = 0, ignore_index = True)

    if verbose: print(f'{bcolors.BOLD_OKCYAN}Done!{bcolors.ENDC}')
    return pddf