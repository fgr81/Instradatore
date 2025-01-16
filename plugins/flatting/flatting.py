import os
import os.path
import logging
import multiprocessing
import xarray as xr
import numpy as np


def togli_time_of_day(root_lavoro, file_in, i):
    """
    Removes the 'time_of_day_24' dimension by collapsing it into the 'time' dimension.

    Args:
        root_lavoro (str): Root directory where the input and output files are located.
        file_in (str): Name of the input NetCDF file.
        i (int): Index for naming the output file.

    Raises:
        Exception: If the dataset cannot be processed.
    """
    file_out = f"{root_lavoro}/flatted_{i:02}.nc"
    logging.debug(f"togli_time_of_day {file_in=} {i=}")
    
    ds = xr.open_dataset(f"{root_lavoro}/{file_in}")

    time_values = ds['time'].values
    time_of_day_values = ds['time_of_day_24'].values

    # Create new time dimension by combining 'time' and 'time_of_day_24'
    new_time = []
    for t in time_values:
        new_time.extend(t + time_of_day_values / 24.0)
    new_time = np.array(new_time)

    # Collapse variables with 'time' and 'time_of_day_24' dimensions
    new_data_vars = {}
    for var_name, var_data in ds.data_vars.items():
        if 'time' in var_data.dims and 'time_of_day_24' in var_data.dims:
            # Transpose and reshape to collapse dimensions
            new_shape = (-1,) + var_data.shape[2:]  # Collapse first two dimensions
            new_data_vars[var_name] = (('time',) + var_data.dims[2:], var_data.values.reshape(new_shape))
        else:
            new_data_vars[var_name] = (var_data.dims, var_data.values)

    # Create new dataset with updated variables and coordinates
    new_ds = xr.Dataset(
        data_vars=new_data_vars,
        coords={'time': new_time, 'lat': ds['lat'], 'lon': ds['lon'], 'zagl': ds['zagl']}
    )

    # Preserve attributes and save output
    new_ds.attrs = ds.attrs
    new_ds.to_netcdf(file_out)


class Flatting():
    """
    A class to handle flattening of atmospheric simulation data by collapsing dimensions.

    Attributes:
        env (dict): Environment variables containing processing parameters.
    """

    def __init__(self, report, **env):
        """
        Initializes the Flatting class with the given report and environment variables.

        Args:
            report (object): Report object for logging output messages.
            **env (dict): Environment variables containing simulation parameters.

        Raises:
            KeyError: If required environment variables are missing.
        """
        self.env = env
        logging.debug(f"class Flatting(), {self.env=}")
    
        report_header = self.__class__.__init__
        report_msg = ""
        err = 0
        
        out_type = self.env['out_type']
        z_type = self.env['z_type']
        root_lavoro = self.env['root_lavoro']
        periodi = self.env['periodi']
        max_threads = self.env['max_threads']

        pool = multiprocessing.Pool(processes=max_threads)
        tasks = []

        if out_type == 'diurn':
            logging.debug(f"{out_type=}")
            logging.debug(f"Collapsing 'time_of_day_24' dimension into 'time'")
            for i in range(len(periodi) - 1):
                current = periodi[i]
                _next = periodi[i + 1]
                files = [f for f in os.listdir(root_lavoro) if f.endswith(f"atmos_{out_type}_Ls{current}_{_next}.nc")]
                for file in files:
                    prefix = file.split('.')[0]
                tasks.append(pool.apply_async(
                    togli_time_of_day,
                    args=(root_lavoro, f"{prefix}.atmos_{out_type}_Ls{current}_{_next}_{z_type}_sel.nc", i)
                ))
            pool.close()

            for task in tasks:
                task.get()  # Wait for all tasks to complete
            pool.join()

            logging.debug('Flattening completed.')

