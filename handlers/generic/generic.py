import os
import logging


class Generic:
    """
    A generic class for handling environment variables and reporting.

    Attributes:
        env (dict): A dictionary of environment variables passed during initialization.
    """

    def __init__(self, report, **env):
        """
        Initializes the Generic class with environment variables and logs the information.

        Args:
            report (object): A report object with an `add` method to log environment details.
            **env: Arbitrary keyword arguments representing environment variables.
        """
        self.env = env
        print(self)
        logging.debug(f"sono dentro Generic(), environment variables: {self.env}")
        report.add(f"{self.env}")

    def __str__(self):
        """
        String representation of the Generic class.

        Returns:
            str: String representation of the environment variables.
        """
        return f"{self.env}"

