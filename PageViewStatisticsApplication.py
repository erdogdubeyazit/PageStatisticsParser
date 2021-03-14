from core.Template import ApplicationTemplate
from core.AggregationHelper import PageViewDurationHelper, PageViewFrequencyHelper
from core.ArgumentHelper import PageViewStaticticsApplicationInputParameterParser
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging


class PageViewStaticticsApplication(ApplicationTemplate):
    """
    Page views recency, frequency parser application.

    Data source
    -----------
    * fact.csv :   In "input_data" folder     
        - USER_ID       : Integer    
        - VIEW_TIME     : String    Format : "dd/mm/yyyy hh:mm"
        - WEB_PAGEID    : Integer   

    * lookup.csv :  In "input_data" folder
                    Assumed to be small enough to fit into memory. It is boradcast joined
        - WEB_PAGEID    : Integer
        - WEBPAGE_TYPE  : String   

    Data target
    -----------
    Exports(overwrites) data into resut folder as partitioned by user_id. 

    Input Parameters
    ----------------
    Currently depends on ArgumentHelper.InputParameterParser. 
    Please see details in InputParameterParser.

    """

    def __init__(self, spark=None, logger=None, pageTypes=[], isDuration=True, isFrequency=True, timeWindows=[], dateOfReference=None):
        """
        Fields
        ------
        * pageTypes         : WEBPAGE_TYPE values.
        * isDuration        : True if 'dur' metrictype passed.
        * isFrequency       : True if 'fre' metrictype passed.
        * timeWindows       : Integer values for frequency calculations.
        * dateofreference   : String value for recency calculation. Format : "dd/mm/yyyy"

        """
        self.pageTypes = pageTypes
        self.isDuration = isDuration
        self.isFrequency = isFrequency
        self.timeWindows = timeWindows
        self.dateOfReference = dateOfReference
        if(
            len(pageTypes) == 0 or (isDuration and dateOfReference == None)
            or (isFrequency == True and len(timeWindows) == 0)

        ):
            raise Exception("Inavlid input parameters")
        super().__init__(spark, logger)

    def handleRead(self):
        factDF = self.getSpark().read.format("csv").option(
            "header", True).option("inferSchema", True).load("input_data/fact.csv")
        lookupDF = self.getSpark().read.format(
            "csv").option("header", True).load("input_data/lookup.csv")

        dataFrame = self.joinFactAndLookupData(
            factDF, lookupDF, "WEB_PAGEID")
        dataFrame = self.selectColumnsFromJoinedFactAndLookupData(dataFrame)

        self.dataFrame = dataFrame

    def joinFactAndLookupData(self, factDF, lookupDF, joinColumn):
        joinColumnExist = True
        try:
            factDF[joinColumn]
            lookupDF[joinColumn]
        except:
            joinColumnExist = False

        if(joinColumnExist == False):
            raise Exception(
                "DataFrames to join must include 'WEB_PAGEID' column.")

        # lookupDF is small enough to fit in the memory
        return factDF.join(F.broadcast(lookupDF), [joinColumn])

    def selectColumnsFromJoinedFactAndLookupData(self, dataFrame):
        return dataFrame.select("USER_ID", "WEBPAGE_TYPE", "WEB_PAGEID", F.to_date(
            "VIEW_TIME", "dd/MM/yyyy HH:mm").alias("VIEW_TIME"))

    def handleProcess(self):
        # Repartition dataFrame based on USER_ID and WEB_PAGEID for gathering keys for aggregation.
        # Since WEB_PAGEID is sortable (integer type), WEB_PAGEID is prefered for repartition instead of WEBPAGE_TYPE, in case of any need for sor merge join.
        # Date parts are for salting due to skew in 'lookup' data
        self.dataFrame = self.dataFrame.repartition("USER_ID", F.col("WEB_PAGEID"), F.year(
            "VIEW_TIME"), F.month("VIEW_TIME"), F.dayofmonth("VIEW_TIME"))

        self.dataFrame.persist()

        aggregateColumns = []
        selectColumns = []
        if self.isDuration:
            pageViewDurationHelper = PageViewDurationHelper(
                self.dateOfReference, self.pageTypes)
            aggregateColumns += pageViewDurationHelper.getAggregateColumnExpressions()
            selectColumns += pageViewDurationHelper.getSelectColumnExpressions()
        if self.isFrequency:
            pageViewFrequencyHelper = PageViewFrequencyHelper(
                self.timeWindows, self.pageTypes)
            aggregateColumns += pageViewFrequencyHelper.getAggregateColumnExpressions()
            selectColumns += pageViewFrequencyHelper.getSelectColumnExpressions()

        if self.isDuration:
            self.dataFrame = self.aggregateWithDuration(
                self.dataFrame, aggregateColumns, selectColumns)
        else:
            self.dataFrame = self.aggregateWithoutDuration(
                self.dataFrame, aggregateColumns, selectColumns)

    def aggregateWithDuration(self, dataFrame, aggregateColumns, selectColumns):
        """Since we are trying to parse data with all WEBPAGE_TYPE values, Web have to add WEBPAGE_TYPE value in groupBy.
        That means null values will also generated due to column combination, and we need to do second aggregation.

        Aggregation logic splitted for performance.
        """
        if(len(aggregateColumns) == 0 or len(selectColumns) == 0):
            raise Exception("Aggregation and select coulmns can not be empty.")
        dataFrame = dataFrame.groupBy(F.col("USER_ID"), F.col("WEBPAGE_TYPE")).agg(
            *aggregateColumns
        ).select(
            F.col("USER_ID").alias("user_id"),
            *selectColumns
        )

        # This second aggregation is because we are trying to get duration satistics with all page types.
        # If we the requirement changes to process only one page type in one execution, we can get rid of this second aggregation.
        dataFrame = dataFrame.groupBy("user_id").agg(
            *[F.sum(column).alias(column) for column in selectColumns])
        return dataFrame

    def aggregateWithoutDuration(self, dataFrame, aggregateColumns, selectColumns):
        """If we don have duration, related with different WEBPAGE_TYPE values, we dont need to include WEBPAGE_TYPE in groupBy.

        """
        if(len(aggregateColumns) == 0 or len(selectColumns) == 0):
            raise Exception("Aggregation and select coulmns can not be empty.")
        dataFrame = dataFrame.groupBy(F.col("USER_ID")).agg(
            *aggregateColumns
        ).select(
            F.col("USER_ID").alias("user_id"),
            *selectColumns
        )
        return dataFrame

    def handleResult(self):
        # Result is aggregated by user_id. "USER_ID" value can be used for disk-partition due to its distribution.
        # DataFrame has already been memory-partitioned.
        self.dataFrame.write.mode("overwrite").partitionBy(
            "user_id").save("result")


if __name__ == '__main__':
    # Real spark instance for the application.
    spark = SparkSession.builder.getOrCreate()
    # logger interface for custom logging.
    logger = logging.getLogger('py4j')

    # See the documentation in the class definition.
    inputParameters = PageViewStaticticsApplicationInputParameterParser()

    PageViewStaticticsApplication(
        spark,
        logger,
        inputParameters.getPageTypes(),
        inputParameters.isDurationRequested(),
        inputParameters.isFrequencyRequested(),
        inputParameters.getTimeWindows(),
        inputParameters.getTimeOfReference()
    ).run()
