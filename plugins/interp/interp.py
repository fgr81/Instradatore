import subprocess
import os.path
import logging
from instradatore.router import MyException

# Define the path to the shell script to be executed
script_path = f"{os.path.dirname(os.path.realpath(__file__))}/interp.sh"

class Interp:
    """Handles data interpolation by invoking a bash script.

    This class is responsible for running the `interp.sh` script to perform
    interpolation tasks. It uses subprocess to execute the script and manages
    the input environment variables, error handling, and reporting.

    Attributes:
        env (dict): Dictionary containing environment variables required for script execution.
        status (int): Status of the interpolation process, 0 if successful.
    """

    def __init__(self, report, **env):
        """Initializes the Interp class and runs the interpolation process.

        Args:
            report (Report): The report object for logging messages and errors.
            **env: Environment variables required for the interpolation process. Must include:
                - cinterp (str): Interpolation command or configuration.
                - file_dati (str): Path to the input data file.
                - cartella_dati (str): Directory containing data files.
                - root_lavoro (str): Root working directory.

        Raises:
            MyException: If the `interp.sh` script fails or an unexpected error occurs.
        """
        self.env = env
        self.status = 0

        report_header = self.__class__.__name__
        report_msg = ""
        err = 0

        try:
            # Extract required environment variables
            cinterp = self.env['cinterp']
            file_dati = self.env['file_dati']
            cartella_dati = self.env['cartella_dati']
            root_lavoro = self.env['root_lavoro']

            # Execute the bash script with the provided arguments
            result = subprocess.run(
                ["bash", script_path, cinterp, file_dati, cartella_dati, root_lavoro],
                check=True, capture_output=True, text=True
            )
            logging.debug(f"{result.stdout=}")
            report_msg += (f"<br>Processo terminato con successo\n{result.stdout=}")

        except subprocess.CalledProcessError as e:
            logging.debug(f"<br>{e.stderr=}")
            raise MyException(f"Errore nell'esecuzione dello script: {e.stderr}")

        # Add success message to the report
        report.add(report_header, report_msg, err)

