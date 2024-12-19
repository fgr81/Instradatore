import os
import os.path
import logging
import multiprocessing
import sys
import numpy as np
from netCDF4 import Dataset


#def calcola_stress(prefix, current, nextt, root_lavoro, tipo_dati, var_name, z_type):
def calcola_stress(root_lavoro, filein, var_name):

    #file_path = f"{root_lavoro}/{prefix}.atmos_{tipo_dati}_Ls{current}_{nextt}_{z_type}_sel.nc"
    file_path = f"{root_lavoro}/{filein}"
    logging.debug(f"* calcola_stress {file_path=}")
    ds = Dataset(file_path, 'r+')  # Apertura in modalità lettura e scrittura

    #z = ds.variables[z_type][:]
    #u = ds.variables['ucomp_bot'][:]
    #v = ds.variables['vcomp_bot'][:]
    
    if 'rho_f' in ds.variables:
        rho = ds.variables['rho_f'][:]
    else:
        raise Exception(f"Errore critico: Non è presente la variabile rho_f in {file_path}")
    
    if 'ukd' in ds.variables:
        ukd = ds.variables['ukd'][:]
        vkd = ds.variables['vkd'][:]
    elif 'ucomp_bot' in ds.variables:
        ukd = ds.variables['ucomp_bot']
        vkd = ds.variables['vcomp_bot']
    
    if 'stress' in ds.variables:
        stress_o = ds.variables['stress'][:]
    
    if 'co2ice_sfc' in ds.variables:
        co2ice_sfc = ds.variables['co2ice_sfc'][:]
        mask_no_co2 = ( co2ice_sfc == 0.0)    # True dove non c'è co2 ghiacciata
        ukd = np.where(mask_no_co2, ukd, 0.)  
        vkd = np.where(mask_no_co2, vkd, 0.)  
    shear_u = np.zeros((ukd.shape[0], ukd.shape[1], ukd.shape[2]))
    shear_v = np.zeros((vkd.shape[0], vkd.shape[1], vkd.shape[2]))
    K = 0.4
    Z0 = 0.01
    Z1 = 2.
    
    for t in range(ukd.shape[0]):  # Tempo
        for i in range(ukd.shape[1]):  # Latitudinea
            for j in range(ukd.shape[2]):  # Longitudine
                shear_u[t, i, j] = K * (ukd[t,i,j]) / np.log( Z1 / Z0 ) 
                shear_v[t, i, j] = K * (vkd[t,i,j]) / np.log( Z1 / Z0 )

    logging.debug(f"{file_path} Calcolo della magnitudine dello shear")
    shear_magnitude = np.sqrt(shear_u**2 + shear_v**2)

    stress = rho * shear_magnitude**2

    if var_name not in ds.variables:
        stress_var = ds.createVariable(var_name, 'f4', ('time', 'lat', 'lon'), fill_value=np.nan)
        stress_var.units = "Pa"  
        stress_var.long_name = "Wind stress"
    else:
        stress_var = ds.variables[var_name]
    stress_var[:] = stress
    
    '''
    diff = np.abs(stress_o - stress)
    if 'differenza' not in ds.variables:
        diff_var = ds.createVariable('differenza', 'f4', ('time', 'lat', 'lon'), fill_value=np.nan)
    else:
        diff_var = ds.variables['differenza']
    diff_var[:] = diff
    '''

    logging.debug(" *Chiudi il file NetCDF")
    ds.close()


class AddStress():
    def __init__(self, report, **env):
        """ Split atmos_daily simulation datafile to fit memory constraints.

        Args:
            max_threads (int, optional): _description_. Defaults to 5.
        """
        self.env = env
        logging.debug(f"class AddStress(), {self.env=}")
        
        sol_file_dati = self.env['sol_file_dati']
        root_lavoro = self.env['root_lavoro']
        periodi = self.env['periodi']
        if self.env['var_name']:
            var_name = self.env['var_name']
        else:
            var_name = 'stress'

        if os.cpu_count() <= int(self.env['max_threads'] + 1):
            self.env['max_threads'] = os.cpu_count() - 1

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
                    logging.debug(f"Aggiungo task per  {prefix=} {current=} e {nextt=}")
                    filein = f"{prefix}.atmos_{tipo_dati}_Ls{current}_{nextt}_{z_type}_sel.nc"

                #tasks.append(pool.apply_async(calcola_stress, args=(prefix, current, nextt, root_lavoro, tipo_dati, var_name, z_type)))
                tasks.append(pool.apply_async(calcola_stress, args=(root_lavoro, filein, var_name)))

            pool.close()

            for task in tasks:
                task.get()
        
        except Exception as e:
            logging.error(f"Errore durante l'elaborazione: {e}")
            err = 1
            raise Exception(f"{e.stderr}")
        finally:
            pool.join()

#if __name__ == "__main__":
#    calcola_stress('02627', '336', '348', '/cantiere/opendap_data/confronta_stress', 'daily', 'fmg_stress13', 'zagl')
