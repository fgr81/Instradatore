import os
import shutil
import logging
from instradatore.router import MyException

class Fixed:
    """Handles the copying of fixed NetCDF files across multiple periods.

    This class is responsible for iterating through specified periods and copying
    a fixed NetCDF file to destination files that correspond to a specific naming pattern.

    Attributes:
        env (dict): A dictionary containing environment variables for the process.
    """

    def __init__(self, report, **env):
        """Initializes the Fixed class and performs the copying of fixed files.

        Args:
            report (Report): The report object for logging results and errors.
            **env: Environment variables required for the file copying process. Must include:
                - root_lavoro (str): The root working directory.
                - periodi (list): List of periods for file handling.
                - sol_file_dati (str): Base name of the source fixed NetCDF file.
                - out_type (str): Type of data file being processed.

        Raises:
            MyException: If required environment variables are missing or an error occurs during copying.
        """
        self.env = env
        logging.debug(f"class Fixed(), {self.env=}")

        # Set the working directory
        root_lavoro = self.env['root_lavoro']
        os.chdir(root_lavoro)

        periodi = self.env['periodi']
        sol_file_dati_fixed_nc = f"{root_lavoro}/{self.env['sol_file_dati']}.fixed.nc"

        tipo_dati = self.env['out_type']

        report_header = self.__class__.__name__
        report_msg = ""
        err = 0

        # Iterate through periods
        for idx in range(len(periodi) - 1):
            current = periodi[idx]
            next_ = periodi[idx + 1]

            # Find files matching the naming pattern
            files = [f for f in os.listdir(root_lavoro) if f.endswith(f"atmos_{tipo_dati}_Ls{current}_{next_}.nc")]
            logging.debug(f"{files=}")
            if len(files) < 1:
                raise MyException(f"No atmos_{tipo_dati}_Ls{current}_{next_}.nc")

            for file in files:
                prefix = file.split('.')[0]
                destination = f"{root_lavoro}/{prefix}.fixed.nc"
                logging.debug(f"{sol_file_dati_fixed_nc=} {prefix=} {destination=}")

                # Skip copying if the source and destination are the same
                if destination == sol_file_dati_fixed_nc:
                    logging.debug("Source and destination are the same, skipping...")
                    continue

                try:
                    # Copy the fixed file to the destination
                    logging.debug(f"Attempting to copy {sol_file_dati_fixed_nc} to {destination}")
                    shutil.copy2(sol_file_dati_fixed_nc, destination)
                    logging.debug(f"Copied {sol_file_dati_fixed_nc} to {destination}")
                    report_msg += f"<br>Copied {sol_file_dati_fixed_nc} to {destination}"
                except Exception as e:
                    logging.debug(e)
                    raise MyException(f"{e}")

        # Add the result to the report
        report.add(report_header, report_msg)

