import logging
import os.path
import importlib
from datetime import datetime


class Step():

    def __init__(self, step_dict, upper_env):

        self.name = step_dict['name']
        self.type = step_dict['type']
        
        if 'note' in step_dict.keys():
            self.note = step_dict['note']
        else:
            self.note = None
        
        if 'jump' in step_dict.keys():
            self.jump = step_dict['jump']
        else:
            self.jump = None
        
        self.start = step_dict['start']
        self.stop = step_dict['stop']
        
        if 'env' in step_dict.keys():
            self.env = step_dict['env'] 
        else:
            self.env = {}
        self.env.update(upper_env)

    def __str__(self):
        return f"{self.name}, {self.type}, {self.note}, {self.start}, {self.stop}, {self.env}"

    def __repr__(self):
        return self.__str__()

    def run(self, report):
        # Leggo type e instanzio la classe corrispondente
        logging.debug(f"Instanzio {self}")
        _ret = 0
        try:
            module_name = f"handlers.{self.type}"  # adatta 'moduli' alla struttura del tuo progetto
            module = importlib.import_module(module_name)
            #class_name = self.type.capitalize()
            # da snake_case a CamelCase
            class_name = ''.join(word.capitalize() for word in self.type.split('_'))
            logging.debug(f"{class_name=}")
            _step_class = getattr(module, class_name)
            _step = _step_class(report, **self.env)
        except Exception as e:
            logging.debug(f"Eccezione in Step.run(): {e}")
            _ret = -1
            report.add(str(_step_class), e, 1)
            report.finalize
        
        return _ret


class Router():

    def __init__(self, name, folder=None, note=None, report_folder='./', report_name='riiiieport', env=None, steps=None):
        
        self.name = name
        if (folder == None):
            self.folder = os.path.dirname(os.path.realpath(__file__))
        else:
            self.folder = folder
        self.note = note
        self.start = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.end = None
        self.steps = []
        self.report = Report(report_folder, report_name, self)
        if env:
            self.env = env
        else:
            self.env = {}
        if steps:
            for item in steps:
             step = Step(item, self.env)
             self.steps.append(step)

        logging.debug(f"Caricato {self}")
        

    def __str__(self):
        _s = f"{self.name}, {self.folder}, {self.note}, {self.start}, {self.end}"
        for e in self.steps:
            _s += f"\n\t{e}"
        return (_s)

    def __repr__(self):
        return self.__str__()

    def run(self):
        logging.debug(f"Sono dentro Router.run()")
        for s in self.steps:
            logging.debug(f"Faccio il run di {s}")
            if s.jump:
                logging.info(f"{s.name} è in jump")
                next
            else:
                if (s.run(self.report) == 0):
                    pass
                else:
                    logging.debug("STOP")
                    #TODO report che è finito
                    break
        self.stop = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.report.finalize()


class Report():
    '''
    '''
    def __init__(self, folder="./", filename="untitled", router=None):
        self.url = folder + '/' + filename + ".html"
        self.router = router
        logging.debug(f"Creato in memoria il report {self.url}")
        self.message = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha3/dist/css/bootstrap.min.css" rel="stylesheet">
    <title>Report {router.name}</title>
    <style>
        .success {{
            background-color: #d4edda; /* Verde tenue */
            border: 1px solid #c3e6cb;
            color: #155724;
        }}
        .error {{
            background-color: #f8d7da; /* Rosso spento */
            border: 1px solid #f5c6cb;
            color: #721c24;
        }}
    </style>
</head>
<body>
    <div class="container mt-4">
        <h1 class="mb-4">{router.name}</h1>
        <h2 class="mb-4">Start: {router.start}</h2>
        <div class="accordion" id="reportAccordion">
''' 


    def add(self, header, msg, err=0):
        logging.debug(f"Report.add: {msg}")
        data_corrente = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        msg_id = f"msg-{len(self.message)}"  # Unique ID per ogni messaggio

        # Determina la classe CSS in base a err
        css_class = "success" if err == 0 else "error"

        # Aggiungi un elemento all'accordion
        self.message += f'''
            <div class="accordion-item">
                <h2 class="accordion-header" id="{msg_id}-header">
                    <button class="accordion-button collapsed {css_class}" type="button" data-bs-toggle="collapse" data-bs-target="#collapse-{msg_id}" aria-expanded="false" aria-controls="collapse-{msg_id}">
                        {data_corrente} - <b>{header}</b> - {"Successo" if err == 0 else "Errore"}
                    </button>
                </h2>
                <div id="collapse-{msg_id}" class="accordion-collapse collapse" aria-labelledby="{msg_id}-header" data-bs-parent="#reportAccordion">
                    <div class="accordion-body">
                        {msg}
                    </div>
                </div>
            </div>
        '''

    def finalize(self):
        logging.debug(f"Finalizzo il report: \n{self.message}")
        # Chiudi i tag HTML
        self.message += '''
        </div> <!-- Chiude l'accordion -->
    </div> <!-- Chiude il container -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
'''
        with open(self.url, 'w') as f:
            f.write(self.message)
        logging.info(f"Report salvato in {self.url}")

