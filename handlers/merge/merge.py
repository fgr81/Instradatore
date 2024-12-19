import os
import os.path
import logging
import glob
import multiprocessing
import xarray as xr
import numpy as np

class Merge():
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

        if (out_type == 'diurn'):
            file_pattern = os.path.join(root_lavoro, f"flatted_*.nc")
        else:
            file_pattern = os.path.join(root_lavoro, f"*atmos_{out_type}_Ls*_*_{z_type}_sel.nc")
        logging.debug(f"{file_pattern=}")
        
        file_list = sorted(glob.glob(file_pattern))
        logging.debug(f"{file_list=}")
        
        datasets = [xr.open_dataset(file, decode_times=False) for file in file_list]
        ds = xr.concat(datasets, dim='time')  

        logging.debug("Done.")
        ds.to_netcdf(f"{root_lavoro}/atmos_{out_type}_{z_type}_sel.nc")
        logging.debug("File saved")
        
        '''if (out_type == 'diurn'):
            for file_path in glob.glob(file_pattern):
                os.remove(file_path)  # cancella file temporanei
        '''

        report.add(self.__class__.__name__, f"Creato file atmos_{out_type}_{z_type}_sel.nc", err)
