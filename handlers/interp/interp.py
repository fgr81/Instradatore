import subprocess
import os.path
import logging


script_path = f"{os.path.dirname(os.path.realpath(__file__))}/interp.sh"

class Interp():

    def __init__(self, report, **env):
        self.env = env
        self.status = 0
        
        report_header = self.__class__.__name__
        report_msg = ""
        err = 0

        try:
            
            cinterp = self.env['cinterp']
            file_dati = self.env['file_dati']
            cartella_dati = self.env['cartella_dati']
            root_lavoro = self.env['root_lavoro']

            result = subprocess.run(
                    ["bash", script_path, cinterp, file_dati, cartella_dati, root_lavoro],
                    check=True, capture_output=True, text=True
                    )
            logging.debug(f"{result.stdout=}")
            report_msg += (f"<br>Processo terminato con successo\n{result.stdout=}")

        except subprocess.CalledProcessError as e:
            logging.debug(f"<br>{e.stderr=}")
            report_msg += (f"<br>{e.stderr=}")
            err = 1
            report.add(report_header, report_msg, err)
            raise Exception(f"{e.stderr}")

        report.add(report_header, report_msg, err)
