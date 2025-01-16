import logging
import os.path
import importlib
from datetime import datetime

class MyException(Exception):
    def __init__(self, message=None, *args):
        super().__init__(message, *args)
        self.message = message

class Step:
    def __init__(self, step_dict, upper_env):
        if not step_dict.get('name') or not step_dict.get('type'):
            raise ValueError("Both 'name' and 'type' are required in step_dict.")

        self.name = step_dict['name']
        self.type = step_dict['type']
        self.note = step_dict.get('note')
        self.jump = step_dict.get('jump', False)
        self.env = step_dict.get('env', {})
        self.env.update(upper_env)

    def __str__(self):
        return f"{self.name}, {self.type}, {self.note}, {self.env}"

    def __repr__(self):
        return self.__str__()

    def run(self, report):
        logging.debug(f"Instantiating {self}")
        _ret = 0
        try:
            module_name = f"plugins.{self.type}"
            module = importlib.import_module(module_name)
            class_name = ''.join(word.capitalize() for word in self.type.split('_'))
            logging.debug(f"{class_name=}")
            _step_class = getattr(module, class_name)
            _step = _step_class(report, **self.env)
        except MyException as e:
            logging.debug(f"Exception in Step.run(): {e}")
            _ret = -1
            report.add(class_name, e.message, 1)
            report.finalize()
        return _ret

class Router:
    def __init__(self, name, folder=None, note=None, report_folder='./', report_name='riiiieport', env=None, steps=None):
        self.name = name
        self.folder = folder or os.path.dirname(os.path.realpath(__file__))
        self.note = note
        self.start = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.end = None
        self.steps = []
        self.report = Report(report_folder, report_name, self)
        self.env = env or {}
        if steps:
            for item in steps:
                step = Step(item, self.env)
                self.steps.append(step)
        logging.debug(f"Loaded {self}")

    def __str__(self):
        _s = f"{self.name}, {self.folder}, {self.note}, {self.start}, {self.end}"
        for e in self.steps:
            _s += f"\n\t{e}"
        return _s

    def __repr__(self):
        return self.__str__()

    def run(self):
        logging.debug(f"Inside Router.run()")
        for s in self.steps:
            logging.debug(f"Running {s}")
            if s.jump:
                logging.info(f"{s.name} is SKIPPED")
                continue
            else:
                if s.run(self.report) != 0:
                    logging.debug("STOP")
                    break
        self.end = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.report.finalize()

class Report:
    def __init__(self, folder="./", filename="untitled", router=None):
        self.url = f"{folder}/{filename}.html"
        self.router = router
        logging.debug(f"Created report in memory at {self.url}")
        self.message = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha3/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha3/dist/js/bootstrap.bundle.min.js"></script>
    <title>Report {router.name}</title>
</head>
<body>
    <div class="container mt-4">
        <h1 class="mb-4">{router.name}</h1>
        <h2 class="mb-4">Start: {router.start}</h2>
        <div class="accordion" id="reportAccordion">
"""

    def add(self, header, msg, err=0):
        data_corrente = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        css_class = "bg-success text-light" if err == 0 else "bg-danger text-light"
        self.message += f"""
            <div class="accordion-item">
                <h2 class="accordion-header">
                    <button class="accordion-button collapsed {css_class}" type="button" data-bs-toggle="collapse" data-bs-target="#collapse-{header}" aria-expanded="false" aria-controls="collapse-{header}">
                        {data_corrente} - <b>{header}</b> - {"Success" if err == 0 else "Error"}
                    </button>
                </h2>
                <div id="collapse-{header}" class="accordion-collapse collapse" data-bs-parent="#reportAccordion">
                    <div class="accordion-body">{msg}</div>
                </div>
            </div>
        """

    def finalize(self):
        self.message += "</div></body></html>"
        with open(self.url, 'w') as f:
            f.write(self.message)
        logging.info(f"Report saved at {self.url}")

