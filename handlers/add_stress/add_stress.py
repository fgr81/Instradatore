import os
import shutil
import logging
import multiprocessing
import numpy as np
from netCDF4 import Dataset

def calcola_stress(root_lavoro, filein, var_name):
    """Calculates wind stress and writes it to a NetCDF file.

    This function computes the wind stress based on input velocity and density variables.
    The calculated stress is stored in a new or existing variable in the NetCDF file.

    Args:
        root_lavoro (str): The root working directory containing the NetCDF files.
        filein (str): Name of the input NetCDF file.
        var_name (str): Name of the variable to store the calculated stress.

    Raises:
        Exception: If required variables are missing from the NetCDF file.
    """
    file_path = f"{root_lavoro}/{filein}"
    logging.debug(f"* calcola_stress {file_path=}")
    ds = Dataset(file_path, 'r+')  # Open file in read/write mode

    if 'rho_f' in ds.variables:
        rho = ds.variables['rho_f'][:]
    else:
        raise Exception(f"Errore critico: Non è presente la variabile rho_f in {file_path}")
    
    if 'ukd' in ds.variables:
        ukd = ds.variables['ukd'][:]
        vkd = ds.variables['vkd'][:]
    elif 'ucomp_bot' in ds.variables:
        ukd = ds.variables['ucomp_bot'][:]
        vkd = ds.variables['vcomp_bot'][:]

    if 'co2ice_sfc' in ds.variables:
        co2ice_sfc = ds.variables['co2ice_sfc'][:]
        mask_no_co2 = (co2ice_sfc == 0.0)
        ukd = np.where(mask_no_co2, ukd, 0.)
        vkd = np.where(mask_no_co2, vkd, 0.)

    shear_u = np.zeros(ukd.shape)
    shear_v = np.zeros(vkd.shape)
    K = 0.4  # von Kármán constant
    Z0 = 0.01  # Roughness height
    Z1 = 2.0  # Reference height

    for t in range(ukd.shape[0]):  # Time dimension
        for i in range(ukd.shape[1]):  # Latitude
            for j in range(ukd.shape[2]):  # Longitude
                shear_u[t, i, j] = K * (ukd[t, i, j]) / np.log(Z1 / Z0)
                shear_v[t, i, j] = K * (vkd[t, i, j]) / np.log(Z1 / Z0)

    logging.debug(f"{file_path} Calculating shear magnitude")
    shear_magnitude = np.sqrt(shear_u**2 + shear_v**2)
    stress = rho * shear_magnitude**2

    if var_name not in ds.variables:
        stress_var = ds.createVariable(var_name, 'f4', ('time', 'lat', 'lon'), fill_value=np.nan)
        stress_var.units = "Pa"
        stress_var.long_name = "Wind stress"
    else:
        stress_var = ds.variables[var_name]

    stress_var[:] = stress
    logging.debug("Closing NetCDF file")
    ds.close()

class AddStress:
    """Manages the parallel computation of wind stress for multiple periods.

    This class coordinates the calculation of wind stress for multiple time
    periods using multiprocessing and the `calcola_stress` function.

    Attributes:
        env (dict): Environment variables used during processing.
    """

    def __init__(self, report, **env):
        """Initializes the AddStress class and processes files in parallel.

        Args:
            report (Report): The report object for logging results and errors.
            **env: Environment variables required for processing, including:
                - sol_file_dati (str): Base name of the input data file.
                - root_lavoro (str): Root working directory.
                - periodi (list): List of periods for processing.
                - var_name (str): Name of the stress variable (default: "stress").
                - max_threads (int): Maximum number of threads for parallel processing.
                - out_type (str): Type of output data file.
                - z_type (str): Zonal type used in naming conventions.
        """
        self.env = env
        logging.debug(f"class AddStress(), {self.env=}")
        
        sol_file_dati = self.env['sol_file_dati']
        root_lavoro = self.env['root_lavoro']
        periodi = self.env['periodi']
        var_name = self.env.get('var_name', 'stress')

        max_threads = self.env.get('max_threads', os.cpu_count() - 1)
        self.env['max_threads'] = min(max_threads, os.cpu_count() - 1)

        tipo_dati = self.env['out_type']
        z_type = self.env['z_type']

        report_header = self.__class__.__name__
        report_msg = ""

        pool = multiprocessing.Pool(processes=self.env['max_threads'])
        tasks = []

        try:
            for i in range(len(periodi) - 1):
                if tipo_dati == 'diurn':
                    filein = f"flatted_{i:02}.nc"
                else:
                    current = periodi[i]
                    nextt = periodi[i + 1]
                    files = [f for f in os.listdir(root_lavoro) if f.endswith(f"atmos_{tipo_dati}_Ls{current}_{nextt}.nc")]
                    for file in files:
                        prefix = file.split('.')[0]
                    logging.debug(f"Adding task for {prefix=} {current=} and {nextt=}")
                    filein = f"{prefix}.atmos_{tipo_dati}_Ls{current}_{nextt}_{z_type}_sel.nc"

                tasks.append(pool.apply_async(calcola_stress, args=(root_lavoro, filein, var_name)))

            pool.close()

            for task in tasks:
                task.get()
        
        except Exception as e:
            logging.error(f"Error during processing: {e}")
            raise Exception(f"{e}")
        finally:
            pool.join()

