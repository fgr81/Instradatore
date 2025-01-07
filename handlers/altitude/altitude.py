import os
import os.path
import logging
import subprocess
from instradatore.router import MyException

script_path = f"{os.path.dirname(os.path.realpath(__file__))}/altitude.sh"

class Altitude:
    """
    Class to handle the interpolation from pressure to meter altitude system.

    Attributes:
        env (dict): Environment variables required for execution.
    """

    def __init__(self, report, **env):
        """
        Initializes the Altitude class and sets up the environment.

        Args:
            report (object): Report object to log execution details.
            **env: Environment variables passed as keyword arguments.

        Raises:
            MyException: If required environment variables are missing or invalid.
        """
        self.env = env
        logging.debug(f"class Altitude(), {self.env=}")

        # Determine the maximum number of threads
        if os.cpu_count() <= int(self.env['max_threads'] + 1):
            self.env['max_threads'] = os.cpu_count() - 1
        logging.debug(f"Instanzio Altitude con {self.env['max_threads']=}")

        report_header = "Altitude"
        report_msg = ""
        err = 0

        running_processes = []

        required_vars = ['sol_file_dati', 'out_type', 'root_lavoro', 'periodi', 'z_type']
        for var in required_vars:
            if var not in self.env or not self.env[var]:
                raise MyException(f"Missing or invalid environment variable: {var}")

        try:
            sol_file_dati = self.env['sol_file_dati']
            root_lavoro = self.env['root_lavoro']
            periodi = self.env['periodi']
            tipo_file = self.env['out_type']
            tipo_z = self.env['z_type']

            # Launch processes for each period
            for i in range(len(periodi) - 1):
                current = periodi[i]
                _next = periodi[i + 1]

                files = [f for f in os.listdir(root_lavoro) if f.endswith(f"atmos_{tipo_file}_Ls{current}_{_next}.nc")]
                for file in files:
                    prefix = file.split('.')[0]

                logging.debug(f"Lancio MarsInterp con {prefix=} {current=} e {_next=}")

                # Launch process asynchronously
                process = subprocess.Popen(
                    ["bash", script_path, str(prefix), str(tipo_file), str(current), str(_next), str(root_lavoro), str(tipo_z)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                )
                running_processes.append(process)

                # Wait if the number of running processes equals the max threads
                if len(running_processes) >= int(self.env['max_threads']):
                    logging.debug("Raggiunto max_threads, in attesa...")
                    for p in running_processes:
                        stdout, stderr = p.communicate()
                        report_msg += (f"<br>{stdout=} <br>{stderr=}")
                    running_processes.clear()

            # Wait for remaining processes to complete
            for p in running_processes:
                stdout, stderr = p.communicate()
                report_msg += (f"<br>{stdout=} <br>{stderr=}")
            report.add(report_header, report_msg)
        except subprocess.CalledProcessError as e:
            logging.debug(e.stderr)
            raise MyException(f"Errore nell'esecuzione dello script: {e.stderr}")

