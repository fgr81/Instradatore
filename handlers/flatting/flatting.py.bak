import os
import os.path
import logging
import glob
import multiprocessing
import xarray as xr
import numpy as np


def togli_time_of_day(root_lavoro, file_in, i):
    file_out = f"{root_lavoro}/flatted_{i:02}.nc"
    logging.debug(f"togli_time_of_day {file_in=} {i=}")
    
    ds = xr.open_dataset(f"{root_lavoro}/{file_in}")

    time_values = ds['time'].values
    time_of_day_values = ds['time_of_day_24'].values

    new_time = []
    for t in time_values:
        new_time.extend(t + time_of_day_values / 24.0)
    new_time = np.array(new_time)

    new_data_vars = {}
    for var_name, var_data in ds.data_vars.items():
        if 'time' in var_data.dims and 'time_of_day_24' in var_data.dims:
            # Trasponi e collassa time e time_of_day_24 in una singola dimensione
            new_shape = (-1,) + var_data.shape[2:]  # Unisci le prime due dimensioni
            new_data_vars[var_name] = (('time',) + var_data.dims[2:], var_data.values.reshape(new_shape))
        else:
            new_data_vars[var_name] = (var_data.dims, var_data.values)

    new_ds = xr.Dataset(
        data_vars=new_data_vars,
        coords={'time': new_time, 'lat': ds['lat'], 'lon': ds['lon'], 'zagl': ds['zagl']}
    )   

    new_ds.attrs = ds.attrs
    new_ds.to_netcdf(file_out)



class Flatting():
    def __init__(self, report, **env):
        """
        Split atmos_daily simulation datafile to fit memory constraints.

        Args:
            max_threads (int, optional): Numero massimo di thread. Defaults to 5.
        """
        self.env = env
        logging.debug(f"class Merge(), {self.env=}")
    
        report_header = self.__class__.__init__
        report_msg = ""
        err = 0
        
        out_type = self.env['out_type']
        z_type = self.env['z_type']
        root_lavoro = self.env['root_lavoro']
        periodi = self.env['periodi']
        max_threads = self.env['max_threads']

        pool = multiprocessing.Pool(processes = max_threads)
        tasks = []

        if (out_type == 'diurn'):
            logging.debug(f"{out_type=}")
            logging.debug(f"Annullo la dimensione 'time_of_day_2' andando a sommare il suo valore su 'time'")
            for i in range(len(periodi) -1):
                current = periodi[i]
                _next = periodi[i+1]
                files = [f for f in os.listdir(root_lavoro) if f.endswith(f"atmos_{out_type}_Ls{current}_{_next}.nc")]
                for file in files:
                    prefix = file.split('.')[0]
                tasks.append(pool.apply_async(togli_time_of_day, args=(root_lavoro, f"{prefix}.atmos_{out_type}_Ls{current}_{_next}_{z_type}_sel.nc", i)))
            pool.close()

            for task in tasks:
                task.get()  # it waits
            pool.join()

            logging.debug('finito.')
