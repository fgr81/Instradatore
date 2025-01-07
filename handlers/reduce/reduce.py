import os
import os.path
import logging
import glob
import multiprocessing
import xarray as xr
import numpy as np
from instradatore.router import MyException


def riduci_dati(prefix, current, nextt, root_lavoro, out_type, variabili_selezionate, z_type):
    """
    Funzione per ridurre i dati di un file NetCDF.

    Args:
        prefix (str): Prefisso del file.
        current (str): Periodo corrente.
        nextt (str): Periodo successivo.
        root_lavoro (str): Percorso root dei file di lavoro.
    """
    if z_type:
        url = f"{root_lavoro}/{prefix}.atmos_{out_type}_Ls{current}_{nextt}_{z_type}.nc"
        file_dest = f"{root_lavoro}/{prefix}.atmos_{out_type}_Ls{current}_{nextt}_{z_type}_sel.nc"
    else:
        url = f"{root_lavoro}/{prefix}.atmos_{out_type}_Ls{current}_{nextt}.nc"
        file_dest = f"{root_lavoro}/{prefix}.atmos_{out_type}_Ls{current}_{nextt}_sel.nc"

    logging.debug(f"Apro {url}")
    ds = xr.open_dataset(url, decode_times=False)

    OV_VAL = 9.96921e+36

    def first_valid_value(array):
        valid_mask = ~np.isnan(array)
        if np.any(valid_mask):
            return array[valid_mask][0]
        else:
            return np.nan

    '''
    if 'temp' in ds:
        logging.debug('Calculating surf_temp...')
        ds['temp'] = xr.where(ds['temp'] == OV_VAL, np.nan, ds['temp'])
        ds['surf_temp'] = xr.apply_ufunc(
            first_valid_value,
            ds['temp'],
            input_core_dims=[[z_type]],
            vectorize=True,
            output_dtypes=[float]
        )

    if 'rho' in ds:
        logging.debug('Calculating rho_f...')
        ds['rho'] = xr.where(ds['rho'] == OV_VAL, np.nan, ds['rho'])
        ds['rho_f'] = xr.apply_ufunc(
            first_valid_value,
            ds['rho'],
            input_core_dims=[[z_type]],
            vectorize=True,
            output_dtypes=[float]
        )

    if 'd_dz_ucomp' in ds:
        logging.debug('Calculating d_dz_ucomp at the surface...')
        ds['d_dz_ucomp'] = xr.where(ds['d_dz_ucomp'] == OV_VAL, np.nan, ds['d_dz_ucomp'])
        ds['dz_surf_ucomp'] = xr.apply_ufunc(
            first_valid_value,
            ds['d_dz_ucomp'],
            input_core_dims=[[z_type]],
            vectorize=True,
            output_dtypes=[float]
        )

    if 'd_dz_vcomp' in ds:
        logging.debug('Calculating d_dz_vcomp at the surface...')
        ds['d_dz_vcomp'] = xr.where(ds['d_dz_vcomp'] == OV_VAL, np.nan, ds['d_dz_vcomp'])
        ds['dz_surf_vcomp'] = xr.apply_ufunc(
            first_valid_value,
            ds['d_dz_vcomp'],
            input_core_dims=[[z_type]],
            vectorize=True,
            output_dtypes=[float]
        )
    '''

    ###
    # Estraggo le variabili
    ###
    valid_variables = []  
    missing_variables = []  
    for var in variabili_selezionate:
        if var in ds:
            valid_variables.append(var)
        else:
            logging.error(f"Variabile mancante in {url}: {var}")
            missing_variables.append(var)
    if missing_variables:
        raise MyException(f"Le seguenti variabili non esistono nel dataset {url}: {missing_variables}")
    logging.debug(f"Variabili valide selezionate: {valid_variables}")
    
    new_ds = ds[valid_variables]

    # Salvo 

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
    
        report_header = "Reduce"
        report_msg = ""
        self.report = report

        if os.cpu_count() <= int(self.env['max_threads'] + 1):
            self.env['max_threads'] = os.cpu_count() - 1
        logging.debug(f"Instanzio Riduci con {self.env['max_threads']=}")

        required_vars = ['root_lavoro', 'periodi', 'out_type', 'variabili_selezionate']
        for var in required_vars:
            if var not in self.env or not self.env[var]:
                raise MyException(f"Missing or invalid environment variable: {var}")


        root_lavoro = self.env['root_lavoro']
        periodi = self.env['periodi']
        max_threads = int(self.env['max_threads'])
        out_type = self.env['out_type']
        variabili_selezionate = self.env['variabili_selezionate']
        if ('z_type' in self.env):
                z_type = self.env['z_type']
        else:
            z_type = None
        
        logging.debug(f"Reduce {root_lavoro=} {periodi=} {max_threads=} {out_type=} {variabili_selezionate=} {z_type}")

        # Controllo che siano presenti nella root_lavoro i file dati
        current = periodi[0]
        _next = periodi[1]
        files = [f for f in os.listdir(root_lavoro) if f.endswith(f"atmos_{out_type}_Ls{current}_{_next}.nc")]
        if (len(files) < 1):
            raise MyException(f"In {root_lavoro} non è presente il file atmos_{out_type}_Ls{current}_{_next}.nc")
        
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
            logging.error("Errore chiave: %s", e, exc_info=True)
            raise MyException(f"{e}")
        
        finally:
            pool.join()

        report.add(report_header, "Reduce completato con successo")
