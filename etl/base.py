from abc import ABC, abstractmethod


class BaseETL(ABC):
    def __init__(self, logger=None):
        self.logger = logger

    @abstractmethod
    def run(self, *args, **kwargs):
        pass
