import os
import logging
import glob
import xarray as xr
from instradatore.router import MyException


class Merge:
    """Handles merging of NetCDF files for atmospheric simulations.

    This class merges multiple NetCDF files along the time dimension, creating
    a single consolidated dataset. It supports both daily and diurnal data types.

    Attributes:
        env (dict): Environment variables used for processing.
    """

    def __init__(self, report, **env):
        """Initializes the Merge class and performs the merging operation.

        Args:
            report (Report): The report object for logging results and errors.
            **env: Environment variables required for processing, including:
                - out_type (str): Output type (e.g., "daily" or "diurn").
                - z_type (str): Zonal type for file identification.
                - root_lavoro (str): Root working directory containing files.
                - periodi (list): Time periods for merging.
        
        Raises:
            Exception: If no files matching the pattern are found.
        """
        self.env = env
        logging.debug(f"class Merge(), {self.env=}")
    
        report_header = self.__class__.__name__
        report_msg = ""
        err = 0

        required_vars = ['root_lavoro', 'out_type']
        for var in required_vars:
            if var not in self.env or not self.env[var]:
                raise MyException(f"Missing or invalid environment variable: {var}")

        root_lavoro = self.env['root_lavoro']
        out_type = self.env['out_type']
        if 'flatted' in self.env:
            flatted = self.env['flatted']
        else:
            flatted = 0

        if 'z_type' in self.env:
            z_type = self.env['z_type']
        else:
            z_type = None

        # Determine file pattern based on output type
        if out_type == 'diurn' and flatted == 1:
            file_pattern = os.path.join(root_lavoro, "flatted_*.nc")
        else:
            if z_type:
                file_pattern = os.path.join(root_lavoro, f"*atmos_{out_type}_Ls*_*_{z_type}_sel.nc")
            else:
                file_pattern = os.path.join(root_lavoro, f"*atmos_{out_type}_Ls*_*_sel.nc")

        logging.debug(f"Searching for files with pattern: {file_pattern}")
        
        # Find and sort matching files
        file_list = sorted(glob.glob(file_pattern))
        logging.debug(f"Files to merge: {file_list}")

        if not file_list:
            raise MyException(f"No files found matching pattern: {file_pattern}")

        try:
            # Load datasets and concatenate along the time dimension
            datasets = [xr.open_dataset(file, decode_times=False) for file in file_list]
            logging.debug("Concatenating datasets along the time dimension.")
            ds = xr.concat(datasets, dim='time')  

            # Save the merged dataset to a new NetCDF file
            if z_type:
                output_file = f"{root_lavoro}/atmos_{out_type}_{z_type}_sel.nc"
            else:
                output_file = f"{root_lavoro}/atmos_{out_type}_sel.nc"

            logging.debug(f"Saving merged dataset to: {output_file}")
            
            ds.to_netcdf(output_file)
            logging.debug("File saved successfully.")
        
            '''
            if out_type == 'diurn':
                for file_path in glob.glob(file_pattern):
                    os.remove(file_path)  # Delete temporary files
            '''
        except e:
            raise MyException(f"{e}")

        # Add result to the report
        report.add(report_header, f"Created file {output_file}", err)

