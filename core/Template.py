from abc import ABC, abstractmethod
import sys


class ApplicationTemplate(ABC):
    """Template patten for applications.
    Consists of three main phases for reading, processing, and finalizin the process.

    Abstract Methods
    ----------------
    * handleRead    : Phase of reading the data.
    * handleProcess : Phase of transformations. Detail functions will be placed in the concrete class to incerase testability.
    * handleResult  : Phase of completing the application like writing data etc.

    """

    def __init__(self, spark=None, logger=None):
        """
        Fields
        -------------
        * spark     : Pyspark instance injected at instantiation.
        * logger    : Logger instance injected at instantiation.

        """
        if(spark is None):
            raise Exception("Spark instance is null")
        self.spark = spark

        if(logger is None):
            raise Exception("logger is null")
        self.logger = logger
        super().__init__()

    def getSpark(self):
        return self.spark

    def run(self):
        try:
            self.logger.info("Reading...")
            self.handleRead()
            self.logger.info("Reading complete")
        except Exception as e:
            self.logger.error(e)
            self.spark.sparkContext.stop()
            print(e)
            sys.exit(e)
        try:
            self.logger.info("Processing...")
            self.handleProcess()
            self.logger.info("Processing complete")
        except Exception as e:
            self.logger.error(e)
            self.spark.sparkContext.stop()
            print(e)
            sys.exit(e)
        try:
            self.logger.info("Aplication completing...")
            self.handleResult()
            self.logger.info("Application completed")
        except Exception as e:
            self.logger.error(e)
            self.spark.sparkContext.stop()
            print(e)
            sys.exit(e)

    @abstractmethod
    def handleRead(self):
        raise Exception("Not implemented yet")

    @abstractmethod
    def handleProcess(self):
        raise Exception("Not implemented yet")

    @abstractmethod
    def handleResult(self):
        raise Exception("Not implemented yet")
