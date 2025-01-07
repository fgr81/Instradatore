import os
import logging
import subprocess
from instradatore.router import MyException

script_path = f"{os.path.dirname(os.path.realpath(__file__))}/split.sh"

class Split:
    """Class to split atmos_daily simulation datafile to fit memory constraints.

    This class handles the splitting of simulation datafiles into smaller parts
    to fit memory constraints. It utilizes subprocesses to run a bash script
    asynchronously.

    Attributes:
        env (dict): A dictionary containing environment variables for the split process.

    Args:
        report (Report): A report object to log results and errors.
        **env: Environment variables required for the splitting process. Must include:
            - sol_file_dati (str): Path to the input data file.
            - out_type (str): Output file type.
            - root_lavoro (str): Root working directory.
            - periodi (list): List of periods for the split.
            - max_threads (int): Maximum number of threads to use.
    """

    def __init__(self, report, **env):
        self.env = env
        logging.debug(f"class Split(), {self.env=}")

        # Determine the maximum number of threads
        if os.cpu_count() <= int(self.env['max_threads'] + 1):
            self.env['max_threads'] = os.cpu_count() - 1

        running_processes = []
        report_header = "Split"
        report_msg = ""
        err = 0

        try:
            required_vars = ['sol_file_dati', 'out_type', 'root_lavoro', 'periodi']
            for var in required_vars:
                if var not in self.env or not self.env[var]:
                    raise MyException(f"Missing or invalid environment variable: {var}")

            sol_file_dati = self.env['sol_file_dati']
            tipo_file = self.env['out_type']
            root_lavoro = self.env['root_lavoro']
            periodi = self.env['periodi']

            # Launch processes for each period
            for i in range(len(periodi) - 1):  # Avoid IndexError
                current = periodi[i]
                _next = periodi[i + 1]
                logging.debug(f"Launching split.sh with {current=} and {_next=}")

                # Launch the process asynchronously
                process = subprocess.Popen(
                    ["bash", script_path, str(sol_file_dati), str(tipo_file), str(current), str(_next), str(root_lavoro)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                )
                running_processes.append(process)

                # Wait for processes to complete if max_threads are reached
                if len(running_processes) >= int(self.env['max_threads']):
                    logging.debug("Reached max_threads, waiting for processes to complete...")
                    for p in running_processes:
                        stdout, stderr = p.communicate()
                        logging.debug(f"Process {p.pid}, \n stdout: {stdout} \n stderr: {stderr}")
                        report_msg += f"<br>{stdout=}"
                        report_msg += f"<br>{stderr=}"
                        if p.returncode != 0:
                            err = 1
                            raise MyException(f"Error in process {p.pid}: {stderr}")

                    running_processes.clear()  # Clear completed processes

            # Wait for any remaining processes
            for p in running_processes:
                stdout, stderr = p.communicate()
                logging.debug(f"Process {p.pid}, \n stdout: {stdout} \n stderr: {stderr}")
                report_msg += f"<br>{stdout=}"
                report_msg += f"<br>{stderr=}"
                if p.returncode != 0:
                    err = 1
                    raise MyException(f"Error in process {p.pid}: {stderr}")

        except MyException as e:
            raise

        except Exception as e:
            raise MyException(f"Unhandled exception: {e}")

        # Finalize report
        report.add(report_header, report_msg, err)

