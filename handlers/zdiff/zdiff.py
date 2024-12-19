import os
import os.path
import logging
import subprocess

script_path = f"{os.path.dirname(os.path.realpath(__file__))}/zdiff.sh"

class Zdiff():
    def __init__(self, report, **env):
        """ Split atmos_daily simulation datafile to fit memory constraints.

        Args:
            max_threads (int, optional): _description_. Defaults to 5.
        """
        self.env = env
        logging.debug(f"class Zdiff(), {self.env=}")

        # Determina il numero massimo di thread
        if os.cpu_count() <= int(self.env['max_threads'] + 1):
            self.env['max_threads'] = os.cpu_count() - 1
        logging.debug(f"Instanzio Zdiff con {self.env['max_threads']=}")

        report_header = self.__class__.__name__
        report_msg = ""
        err = 0

        running_processes = []
        try:
            sol_file_dati = self.env['sol_file_dati']
            root_lavoro = self.env['root_lavoro']
            periodi = self.env['periodi']
            z = self.env['z_type']
            out_type = self.env['out_type']
            logging.debug(f"{periodi=}")
            logging.debug(f"{len(periodi)=}")

            # Lancia i processi per ogni periodo
            for i in range(len(periodi) - 1):  # Modificato per evitare IndexError
                current = periodi[i]
                _next = periodi[i + 1]
                
                files = [f for f in os.listdir(root_lavoro) if f.endswith(f"atmos_daily_Ls{current}_{_next}.nc")]
                for file in files:
                    prefix = file.split('.')[0]

                logging.debug(f"Lancio MarsVars con {prefix=} {current=} e {_next=}")

                # Lancia il processo in modalità asincrona
                process = subprocess.Popen(
                    ["bash", script_path, str(prefix), str(current), str(_next), str(root_lavoro), str(z), str(out_type)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                )
                running_processes.append(process)

                # Se il numero di processi in esecuzione è uguale al numero massimo di thread, aspetta che terminino
                if len(running_processes) >= int(self.env['max_threads']):
                    logging.debug("Raggiunto max_threads, in attesa...")
                    # Attende che i processi correnti terminino
                    for p in running_processes:
                        stdout, stderr = p.communicate()  # Attende e cattura output
                        report_msg += f"<br>{stdout=} <br>{stderr=}"
                    running_processes.clear()  # Pulisce la lista dei processi in esecuzione

            # Attende il completamento dei processi rimanenti
            for p in running_processes:
                stdout, stderr = p.communicate()
                report_msg += f"<br>{stdout=} <br>{stderr=}"
            report.add(report_header, report_msg, err)

        except subprocess.CalledProcessError as e:
            logging.debug(e.stderr)
            report_msg += f"<br>{e.stderr=}"
            err = 1
            report.add(report_header, report_msg, err)
            raise Exception(f"{e.stderr}")
