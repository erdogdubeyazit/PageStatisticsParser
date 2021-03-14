from abc import ABC, abstractmethod
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType


class AbstractAggregationHelper(ABC):
    """Encapsulates the common information for creating the aggregation columns.

    Fields
    ------
    * dateFormat        :   Date Fromat for to_date() function to parse the dateFormat:string. Default : "dd/MM/yyyy"
    * timeColumn        :   Column name to tract time values. Default   : "VIEW_TIME"
    * categoryColumn    :   Column name to track categories. Default    : "WEBPAGE_TYPE" 

    """

    dateFormat = "dd/MM/yyyy"
    timeColumn = "VIEW_TIME"
    categoryColumn = "WEBPAGE_TYPE"
    __selectColumns = []
    __aggregateColumns = []

    def __init__(self, selectColumns, aggregateColumns):
        self.__selectColumns = selectColumns
        self.__aggregateColumns = aggregateColumns
        super().__init__()

    def getSelectColumnExpressions(self):
        """ Returns select column expressions for dataFrame.select() operation.

        Returns : list
        -------
        Extraction must be considered before injecting directly into dataFrame.select() funtion as parameter.

        Example
        -------
        * dataFrame.select(..., *pageViewDurationHelper.getSelectColumnExpressions(), ...)

        """
        return self.__selectColumns

    def getAggregateColumnExpressions(self):
        """ Returns aggregation column expressions for dataFrame.agg() operation.

        Returns : list
        -------
        Extraction must be considered before injecting directly into dataFrame.agg() funtion as parameter.

        Example
        -------
        * dataFrame.agg(..., *pageViewDurationHelper.getSelectColumnExpressions(), ...)

        """
        return self.__aggregateColumns


class PageViewDurationHelper(AbstractAggregationHelper):

    def __init__(self, dateOfReference=None, pageTypes=None):
        """ Calculates the date difeerenece in days, from the last page visit after a time reference.

        Parameters
        ----------
        * dateOfReference :   str, required
                        The last page visit after this date will be considered. 

        * pageTypes       :   list<str>, required
                        'WEBPAGE_TYPE' values is the 'lookup' table 
        """
        self.checkDateOfReference(dateOfReference)
        self.checkPageTypes(pageTypes)
        self.dateOfReference = dateOfReference
        self.pageTypes = pageTypes
        self.generateExpressions()
        super().__init__(self.selectColumns, self.aggregateColumns)

    def checkDateOfReference(self, dateOfReference):
        if dateOfReference is None:
            raise Exception("Date Of Reference is null")
        if(isinstance(dateOfReference, str) == False):
            raise Exception("Date Of Reference parameter must be string")

    def checkPageTypes(self, pageTypes):
        if pageTypes is None:
            raise Exception("Page Types is null")
        if(isinstance(pageTypes, list) == False):
            raise Exception("Page Types parameter must be list")
        if len(pageTypes) == 0:
            raise Exception("Page Types is empty")
        if(all(isinstance(item, str) for item in pageTypes) == False):
            raise Exception("All Page Types must be string")

    def generateExpressions(self):
        """
        Logic
        -----
        Iterate over Page Type records and generate select and aggredate expressions.

        Example
        -------
        * last_visit_date<=date_of_referenece<=current_date()                            : returns null
        * visit_in_the_past<=last_visit<date_of_referenece<=last_visit<=current_date()   : returns day count between today and last_visit 

        Fields
        ------
        * selectColumn        : "page_view_{pageType}_dur".format(pageType=item)

        * aggregateColumn     : F.when((F.max("VIEW_TIME") >= dateOfReferenceFunction) & (F.col("WEBPAGE_TYPE") == "news"), F.datediff(F.current_date(), F.max("VIEW_TIME")))


        """
        self.selectColumns = ["page_view_{pageType}_dur".format(
            pageType=item) for item in self.pageTypes]

        dateOfReferenceFunction = F.to_date(
            F.lit(self.dateOfReference), self.dateFormat)

        self.aggregateColumns = [F.when(
            (F.max(self.timeColumn) >= dateOfReferenceFunction) & (F.col(self.categoryColumn) == item), F.datediff(
                F.current_date(), F.max(self.timeColumn))
        )
            .alias("page_view_{pageType}_dur".format(
                pageType=item)) for item in self.pageTypes]


class PageViewFrequencyHelper(AbstractAggregationHelper):
    def __init__(self, frequencies=None, pageTypes=None):
        """ Calculates the page visit frequencies depending on 'WEBPAGE_TYPE' values and time window values.

        Parameters
        ----------
        * frequencies :   list<int>, required
                        All prequency values must be int.

        * pageTypes   :   list<str>, required
                        'WEBPAGE_TYPE' values is the 'lookup' table 
        
        """
        self.checkFrequencies(frequencies)
        self.checkPageTypes(pageTypes)
        self.frequencies = frequencies
        self.pageTypes = pageTypes
        self.generateExpressions()
        super().__init__(self.selectColumns, self.aggregateColumns)

    def checkPageTypes(self, pageTypes):
        if pageTypes is None:
            raise Exception("Page Types is null")
        if(isinstance(pageTypes, list) == False):
            raise Exception("Page Types parameter must be list")
        if len(pageTypes) == 0:
            raise Exception("Page Types is empty")
        if(all(isinstance(item, str) for item in pageTypes) == False):
            raise Exception("All Page Types must be string")

    def checkFrequencies(self, frequencies):
        if frequencies is None:
            raise Exception("Frequencies parameter is null")
        if(isinstance(frequencies, list) == False):
            raise Exception("Frequencies parameter must be list")
        if len(frequencies) == 0:
            raise Exception("Frequencies parameter is empty")
        if(all(isinstance(item, int) for item in frequencies) == False):
            raise Exception("All Frequency types must be int")

    def generateExpressions(self):
        """
        Logic
        -----
        Iterate over Page Type records and for every Page Type Iterate over Frequence records.

        * selectColumn        : "page_view_{page}_fre_{freq}".format(page=pageType, freq=frequency)

        * aggregateColumn     : If the 'VIEW_TIME' value is in the time window between now and the day before 'frequency' then produce 1 as int. Then sum all the redords as result
                            F.when((F.datediff(F.current_date(), F.col("VIEW_TIME")) <= frequency) & (F.col("WEBPAGE_TYPE") == pageType), 1).otherwise(0))
        
        """
        self.selectColumns = []
        self.aggregateColumns = []
        for pageType in self.pageTypes:
            for frequency in self.frequencies:
                self.selectColumns.append("page_view_{page}_fre_{freq}".format(
                    page=pageType, freq=frequency))
                self.aggregateColumns.append(F.sum(
                    F.when(
                        (F.datediff(F.current_date(), F.col(self.timeColumn)
                                    ).cast(IntegerType()) <= frequency) & (F.col(self.categoryColumn) == pageType), 1
                    ).otherwise(0)
                ).alias("page_view_{page}_fre_{freq}".format(
                    page=pageType, freq=frequency)))
