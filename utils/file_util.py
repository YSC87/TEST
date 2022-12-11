import logging
import functools
import pandas as pd
import os
import abc
import utils.misc_util as miscu


class DataStorage(abc.ABC):

    def __init__(self):
        logging.info("Data Storage Object is created.")

    @abc.abstractmethod
    def read(self, config):
        pass

    @abc.abstractmethod
    def write(self, config):
        pass


class FileDataStorage(DataStorage):

    def __init__(self):
        super().__init__()

    @staticmethod
    def validate_path(path, is_directory=False, pop_error=True):
        """
        Validate provided path.
        :param path: Fully qualified file path
        :param is_directory: bool; default=False; The path is a directory path if True else a file path
        :param pop_error: bool; default=True; Whether to interrupt the function when the file path doesn't exist
        :return: bool; Resulted validation; either true or raise an exception
        """
        if not is_directory and not os.path.isfile(path):
            if pop_error:
                logging.error(f'Provided file path is invalid: <{path}>')
                raise FileNotFoundError(f'Provided directory path is invalid: <{path}>')
            else:
                return False
        elif is_directory and not os.path.isdir(path):
            logging.error(f'Provided directory path is invalid: <{path}>')
            raise FileNotFoundError(f'Provided directory path is invalid: <{path}>')
        return True

    def read(self, config):
        """Read file, along with validating provided path.
        :param config: dict; provided configuration mapping
        :return: pd.DataFrame; Resulted dataframe
        """
        description = miscu.eval_elem_mapping(config, 'description')
        path = miscu.eval_elem_mapping(config, 'path')
        file_type = miscu.eval_elem_mapping(config, 'file_type', default_value='excel')
        separator = miscu.eval_elem_mapping(config, 'separator', default_value=',')
        skip_rows = miscu.eval_elem_mapping(config, 'skip_rows', default_value=0)
        use_cols = miscu.eval_elem_mapping(config, 'use_cols', default_value=None)
        sheet_name = miscu.eval_elem_mapping(config, 'sheet_name', default_value=0)
        df_target = None

        if self.validate_path(path):
            if file_type.lower() == 'csv':
                # Read csv based file.
                df_target = pd.read_csv(path, sep=separator, skiprows=skip_rows, usecols=use_cols)
            elif file_type.lower() == 'excel':
                # Read Excel based file.
                if len((pd.ExcelFile(path)).sheet_names) > 1:
                    df_target = pd.read_excel(path, skiprows=skip_rows, usecols=use_cols, sheet_name=sheet_name)
                else:
                    df_target = pd.read_excel(path, skiprows=skip_rows, usecols=use_cols)

        logging.info(f'{description} records <{len(df_target.index)}> were read from <{path}>')
        return df_target

    def write(self, df_output, config):
        """Write file, along with validating provided path.
        :param config: dict; provided configuration mapping
        :param df_output: pd.Dataframe; the output dataframe
        :return: str; the path of the file
        """
        description = miscu.eval_elem_mapping(config, 'description')
        df = df_output
        path = miscu.eval_elem_mapping(config, 'path')
        file_type = miscu.eval_elem_mapping(config, 'file_type', default_value='csv')
        separator = miscu.eval_elem_mapping(config, 'separator', default_value=',')
        mode = miscu.eval_elem_mapping(config, 'mode', default_value='new')
        index = miscu.eval_elem_mapping(config, 'index', default_value='False') == "True"

        if file_type.lower() == 'excel':
            write_function = functools.partial(df.to_excel, index=index)
        elif file_type.lower() == 'csv':
            write_function = functools.partial(df.to_csv, sep=separator, index=index)

        if mode == "overwrite":
            # Check whether the file exists
            if self.validate_path(path, is_directory=False):
                write_function(path)
                logging.info(f'{description} records <{len(df)}> were overwritten into <{path}>')
        elif mode == 'new':
            # Divide the path into directory + filename
            directory, file_name = path.rsplit("/", 1)
            # Divide the filename into stem + extension
            stem, ext = file_name.rsplit(".", 1)
            # Check whether the directory is valid
            if self.validate_path(directory, is_directory=True):
                suffix = 1
                # Loop until the file name with the appropriate suffix is found
                while self.validate_path(os.path.join(directory, file_name), pop_error=False):
                    new_stem = stem + "_" + str(suffix)
                    suffix += 1
                    file_name = ".".join((new_stem, ext))
                path = os.path.join(directory, file_name)
                write_function(path)
                logging.info(f'{description} records <{len(df)}> were written into <{path}>')
        else:
            logging.error(f'The mode is undefined.')
            raise NotImplementedError(f'Mode <{mode}> is not defined!')
        return path


# def read(description, path, file_type='excel', separator=',', skip_rows=0, use_cols=None, sheet_name=0):
#     """
#     Read file, along with validating provided path.
#     :param description: str; File description
#     :param path: str; Fully qualified file name to read
#     :param file_type: str, default='Excel'; Read type with possible values of 'csv' or 'excel'
#     :param separator: str, default=','; Values separator
#     :param skip_rows: int, default=0; Number of rows to skip
#     :param use_cols: int, default=None; A list of columns to read (all others are discarded)
#     :param sheet_name: int or str; default=0; A sheet name or index to read
#     :return: pd.DataFrame; Resulted dataframe
#     """
#     df_target = None
#     if validate_path(path):
#         if file_type.lower() == 'csv':
#             # Read csv based file.
#             df_target = pd.read_csv(path, sep=separator, skiprows=skip_rows, usecols=use_cols)
#         elif file_type.lower() == 'excel':
#             # Read Excel based file.
#             if len((pd.ExcelFile(path)).sheet_names) > 1:
#                 df_target = pd.read_excel(path, skiprows=skip_rows, usecols=use_cols, sheet_name=sheet_name)
#             else:
#                 df_target = pd.read_excel(path, skiprows=skip_rows, usecols=use_cols)
#
#     logging.info(f'{description} records <{len(df_target.index)}> were read from <{path}>')
#     return df_target
#
#
# def write(description, df, path, file_type='Excel', separator=',', mode='overwrite', index=False):
#     """
#     Write file, along with validating provided path.
#     :param description: str; File description
#     :param df: pd.DataFrame; Inputted dataframe
#     :param path: str; File name to write
#     :param file_type: str, default='Excel'; Write type with possible values of 'csv' or 'excel'
#     :param separator: str, default=','; Values separator
#     :param mode: str; default='overwrite'; specify whether to overwrite or create new file
#     :param index: bool; default=False; specify whether to write the index into the file
#     :return: str; the path of the file
#     """
#     if file_type.lower() == 'excel':
#         write_function = functools.partial(df.to_excel, index=index)
#     elif file_type.lower() == 'csv':
#         write_function = functools.partial(df.to_csv, sep=separator, index=index)
#
#     if mode == "overwrite":
#         # Check whether the file exists
#         if validate_path(path, is_directory=False):
#             write_function(path)
#             logging.info(f'{description} records <{len(df)}> were overwritten into <{path}>')
#     elif mode == 'new':
#         # Divide the path into directory + filename
#         directory, file_name = path.rsplit("/", 1)
#         # Divide the filename into stem + extension
#         stem, ext = file_name.rsplit(".", 1)
#         # Check whether the directory is valid
#         if validate_path(directory, is_directory=True):
#             suffix = 1
#             # Loop until the file name with the appropriate suffix is found
#             while validate_path(os.path.join(directory, file_name), pop_error=False):
#                 new_stem = stem + "_" + str(suffix)
#                 suffix += 1
#                 file_name = ".".join((new_stem, ext))
#             path = os.path.join(directory, file_name)
#             write_function(path)
#             logging.info(f'{description} records <{len(df)}> were written into <{path}>')
#     else:
#         logging.error(f'The mode is undefined.')
#         raise NotImplementedError(f'Mode <{mode}> is not defined!')
#     return path
#
#
# def validate_path(path, is_directory=False, pop_error=True):
#     """
#     Validate provided path.
#     :param path: Fully qualified file path
#     :param is_directory: bool; default=False; The path is a directory path if True else a file path
#     :param pop_error: bool; default=True; Whether to interrupt the function when the file path doesn't exist
#     :return: bool; Resulted validation; either true or raise an exception
#     """
#     if not is_directory and not os.path.isfile(path):
#         if pop_error:
#             logging.error(f'Provided file path is invalid: <{path}>')
#             raise FileNotFoundError(f'Provided directory path is invalid: <{path}>')
#         else:
#             return False
#
#     elif is_directory and not os.path.isdir(path):
#         logging.error(f'Provided directory path is invalid: <{path}>')
#         raise FileNotFoundError(f'Provided directory path is invalid: <{path}>')
#
#     return True
