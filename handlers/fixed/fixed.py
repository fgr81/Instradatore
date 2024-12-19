import os
import shutil
import logging


class Fixed():
    def __init__(self, report, **env):
        self.env = env
        logging.debug(f"class Fixed(), {self.env=}")

        root_lavoro = self.env['root_lavoro']
        os.chdir(root_lavoro)

        periodi = self.env['periodi']
        sol_file_dati_fixed_nc = f"{root_lavoro}/{self.env['sol_file_dati']}.fixed.nc"

        tipo_dati = self.env['out_type']

        report_header = self.__class__.__name__
        report_msg = ""
        err = 0

        # Iterazione sui periodi
        for idx in range(len(periodi) - 1):
            current = periodi[idx]
            next_ = periodi[idx + 1]
            # Lista di file che corrispondono al pattern
            files = [f for f in os.listdir(root_lavoro) if f.endswith(f"atmos_{tipo_dati}_Ls{current}_{next_}.nc")]
            logging.debug(f"{files=}")
            for file in files:
                prefix = file.split('.')[0]
                destination = f"{root_lavoro}/{prefix}.fixed.nc"
                logging.debug(f"{sol_file_dati_fixed_nc=} {prefix=}  {destination=}")
                try:
                    #if not os.path.samefile(sol_file_dati_fixed_nc, destination):
                    logging.debug(f"Provo a copiare {sol_file_dati_fixed_nc} in {destination}")
                    shutil.copy2(sol_file_dati_fixed_nc, destination)
                    logging.debug(f"Copiato {sol_file_dati_fixed_nc} in {destination}")
                    report_msg += (f"<br>Copiato {sol_file_dati_fixed_nc} in {destination}")
                    #else:
                    #    logging.debug("I file sono gli stessi. Copia non necessaria.")
                    #    report_msg += (f"<br>{sol_file_dati_fixed_nc} è già in {destination}")
                except Exception as e:
                    logging.debug(e)
                    report_msg += f"<br>{e}"
                    #err = 1
                    #report.add(report_header, report_msg, err)
                    #raise Exception(f"{e.stderr}")  

        report.add(report_header, report_msg)




