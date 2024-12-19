import os
import os.path
import logging
import glob
import multiprocessing
import xarray as xr
import numpy as np

def riduci_dati(prefix, current, nextt, root_lavoro, out_type, variabili_selezionate, z_type):
    """
    Funzione per ridurre i dati di un file NetCDF.

    Args:
        prefix (str): Prefisso del file.
        current (str): Periodo corrente.
        nextt (str): Periodo successivo.
        root_lavoro (str): Percorso root dei file di lavoro.
    """
    url = f"{root_lavoro}/{prefix}.atmos_{out_type}_Ls{current}_{nextt}_{z_type}.nc"
    logging.debug(f"Apro {url}")
    ds = xr.open_dataset(url, decode_times=False)

    OV_VAL = 9.96921e+36

    def first_valid_value(array):
        valid_mask = ~np.isnan(array)
        if np.any(valid_mask):
            return array[valid_mask][0]
        else:
            return np.nan

    if 'temp' in ds:
        logging.debug('Calculating surf_temp...')
        ds['temp'] = xr.where(ds['temp'] == OV_VAL, np.nan, ds['temp'])
        ds['surf_temp'] = xr.apply_ufunc(
            first_valid_value,
            ds['temp'],
            #input_core_dims=[['zstd']],
            input_core_dims=[[z_type]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float]
        )

    if 'rho' in ds:
        logging.debug('Calculating rho_f...')
        ds['rho'] = xr.where(ds['rho'] == OV_VAL, np.nan, ds['rho'])
        ds['rho_f'] = xr.apply_ufunc(
            first_valid_value,
            ds['rho'],
            #input_core_dims=[['zstd']],
            input_core_dims=[[z_type]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float]
        )

    if 'd_dz_ucomp' in ds:
        logging.debug('Calculating d_dz_ucomp at the surface...')
        ds['d_dz_ucomp'] = xr.where(ds['d_dz_ucomp'] == OV_VAL, np.nan, ds['d_dz_ucomp'])
        ds['dz_surf_ucomp'] = xr.apply_ufunc(
            first_valid_value,
            ds['d_dz_ucomp'],
            #input_core_dims=[['zstd']],
            input_core_dims=[[z_type]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float]
        )

    if 'd_dz_vcomp' in ds:
        logging.debug('Calculating d_dz_vcomp at the surface...')
        ds['d_dz_vcomp'] = xr.where(ds['d_dz_vcomp'] == OV_VAL, np.nan, ds['d_dz_vcomp'])
        ds['dz_surf_vcomp'] = xr.apply_ufunc(
            first_valid_value,
            ds['d_dz_vcomp'],
            #input_core_dims=[['zstd']],
            input_core_dims=[[z_type]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float]
        )

    #variabili_selezionate = ds[['rho', 'rho_f', 'stress', 'ukd', 'vkd', 'ps', 'surf_temp', 'co2ice_sfc', 'dz_surf_ucomp', 'dz_surf_vcomp']]
    
    new_ds = ds[variabili_selezionate]

    file_dest = f"{root_lavoro}/{prefix}.atmos_{out_type}_Ls{current}_{nextt}_{z_type}_sel.nc"

    logging.debug(f"Salvo {file_dest}")
    new_ds.to_netcdf(file_dest)

class Reduce():
    def __init__(self, report, **env):
        """
        Split atmos_daily simulation datafile to fit memory constraints.

        Args:
            max_threads (int, optional): Numero massimo di thread. Defaults to 5.
        """
        self.env = env
        logging.debug(f"class Riduci(), {self.env=}")
    
        report_header = self.__class__.__init__
        report_msg = ""
        err = 0

        if os.cpu_count() <= int(self.env['max_threads'] + 1):
            self.env['max_threads'] = os.cpu_count() - 1
        logging.debug(f"Instanzio Riduci con {self.env['max_threads']=}")

        self.report = report

        root_lavoro = self.env['root_lavoro']
        periodi = self.env['periodi']
        max_threads = int(self.env['max_threads'])
        out_type = self.env['out_type']
        variabili_selezionate = self.env['variabili_selezionate']
        z_type = self.env['z_type']

        pool = multiprocessing.Pool(processes=max_threads)
        tasks = []

        try:
            for i in range(len(periodi) - 1):
                current = periodi[i]
                _next = periodi[i + 1]

                files = [f for f in os.listdir(root_lavoro) if f.endswith(f"atmos_{out_type}_Ls{current}_{_next}.nc")]
                for file in files:
                    prefix = file.split('.')[0]

                logging.debug(f"Aggiungo task per {prefix=}, {current=}, {_next=}")

                tasks.append(pool.apply_async(riduci_dati, args=(prefix, current, _next, root_lavoro, out_type, variabili_selezionate, z_type)))

            pool.close()

            for task in tasks:
                task.get()  # Attende la fine di ogni task e raccoglie eccezioni

        except Exception as e:
            logging.error(f"Errore durante l'elaborazione: {e}")
            err = 1
            raise Exception(f"{e.stderr}")
        finally:
            pool.join()

