import os
import logging


class Generic():

    def __init__(self, report, **env):
        self.env = env
        print(self)
        logging.debug(f"sono dentro Generic(), environment variables: {self.env}")
        report.add(f"{self.env}")

    def __str__(self):
        return f"{self.env}"
