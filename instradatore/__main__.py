import rich_click as click
from rich.progress import SpinnerColumn, Progress
import multiprocessing
import yaml
import logging
import os
from os.path import exists
import sys

from instradatore.router import Router

def make_url(folder, filename):
    return f"{folder}/{filename}.yaml"

def daemonize():
    """Trasforma il processo corrente in un daemon."""
    if os.fork():  # Primo fork
        os._exit(0)
    os.setsid()  # Crea una nuova sessione
    if os.fork():  # Secondo fork
        os._exit(0)
    # Reindirizza standard input, output, error
    sys.stdin.flush()
    sys.stdout.flush()
    sys.stderr.flush()
    with open('/dev/null', 'r') as devnull:
        os.dup2(devnull.fileno(), sys.stdin.fileno())
    with open('/dev/null', 'a') as devnull:
        os.dup2(devnull.fileno(), sys.stdout.fileno())
        os.dup2(devnull.fileno(), sys.stderr.fileno())


@click.group()
@click.option('--log', default='log/instradatore.log', help='Log file location')
@click.option('--folder', help='The folder in which save the chain.', default='./')
@click.option('-d', '--daemon', is_flag=True, help='Run in background mode')
@click.pass_context
def cli(ctx, log, daemon, folder):
    # Configura il logging
    setup_logging(log)
    ctx.obj = {'log': log, 'daemon': daemon, 'folder':folder}
    logging.debug(f"{daemon=}")

def setup_logging(log_filename):
    """Configura il file di log."""
    logging.basicConfig(filename=log_filename,
                        level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(filename)s - Line %(lineno)d - %(message)s'
                        )
    logging.debug("Logging configurato correttamente")


@cli.command("init")
@click.argument('name')
@click.pass_context
def init_router(ctx, name):
    daemon = ctx.obj.get('daemon', False)
    folder = ctx.obj.get('folder', './')
    _url = make_url(folder, name)

    if exists(_url):
        if not daemon and click.confirm(f"Il file '{_url}' esiste già. Vuoi inizializzarlo?", default=True):
            # Initialize the chain
            with open(_url, 'r') as yaml_file:
                _data = yaml.safe_load(yaml_file)
                logging.info(f"File {_url} caricato")
                _router = Router(**_data)
                # Set all start(s) and stop(s) to None
                _router.start = None
                _router.stop = None
                for step in _router.steps:
                    step.start = None
                    step.stop = None
            with open(_url, 'w') as f:
                yaml.dump(_router.to_dict(), f)
        elif daemon:
            logging.info(f"Modalità daemon: il file '{_url}' sarà inizializzato automaticamente.")
        else:
            logging.info("L'utente ha scelto di non caricare il file YAML.")
            print("File non caricato.\nEsco.")
    else:
        # Creare il file se non esiste (aggiungi la logica qui)
        #TODO
        logging.info(f"Creazione del file '{_url}' non implementata.")


@cli.command("start")
@click.argument('name')
@click.pass_context
def start_router(ctx, name):

    logging.debug("start_router")
    daemon = ctx.obj.get('daemon', False)
    folder = ctx.obj.get('folder', './')
    _url = make_url(folder, name)
    logging.debug(f"Start {_url}")
    
    worker(_url,daemon)


def worker(_url, daemon):
    
    logging.debug("worker")

    if daemon:
        daemonize()

    if exists(_url):
        with open(_url, 'r') as yaml_file:
            _data = yaml.safe_load(yaml_file)
            logging.info(f"File {_url} caricato")
            _router = Router(**_data)

            if not daemon:
                with Progress(SpinnerColumn(), transient=True) as progress:
                    task = progress.add_task("[cyan]Avviando il router...", start=False)
                    progress.start_task(task)
                    try:
                        _router.run()
                    except Exception as e:
                        logging.error(f"Errore durante l'esecuzione: {e}")
                        progress.stop_task(task)
                        raise
            else:
                try:
                    _router.run()
                except Exception as e:
                    logging.error(f"Errore durante l'esecuzione: {e}")
    else:
        if not daemon:
            print("File non trovato")
        logging.error(f"File '{_url}' non trovato.")

if __name__ == '__main__':
    cli()

