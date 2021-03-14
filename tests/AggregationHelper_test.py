from pyspark.sql.types import IntegerType
from core.AggregationHelper import AbstractAggregationHelper, PageViewDurationHelper, PageViewFrequencyHelper
import unittest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

class PageViewDurationHelper_test(unittest.TestCase):

    def setUp(self) -> None:
        return SparkSession.builder.master("local[2]").appName("PageViewDurationHelper_test").getOrCreate()

    def test_must_be_subclass_of_AbstractAggregationHelper(self):
        object = PageViewDurationHelper("01/01/2000", ["news"])
        self.assertIsInstance(object, AbstractAggregationHelper,
                              "PageViewDurationHelper must be inherited from AbstractAggregationHelper")

    def test_empty_dateOfReference_shoulf_fail(self):
        with self.assertRaises(Exception):
            PageViewDurationHelper(None, ["news"])

    def test_empty_pageTypes_should_fail(self):
        with self.assertRaises(Exception):
            PageViewDurationHelper("01/01/2020", [])

    def test_expression_generation(self):
        pageTypes = ["news", "movies"]
        dateOfReference = "01/01/2020"
        helper = PageViewDurationHelper(dateOfReference, pageTypes)
        helper.generateExpressions()

        expectedSelectColumns = ["page_view_{pageType}_dur".format(
            pageType=item) for item in pageTypes]
        self.assertEquals(helper.getSelectColumnExpressions(),
                          expectedSelectColumns)

        dateOfReferenceFunction = F.to_date(
            F.lit(dateOfReference), helper.dateFormat)
        expectedAggregateColumns = [str(F.when(
            (F.max(helper.timeColumn) >= dateOfReferenceFunction) & (F.col(helper.categoryColumn) == item), F.datediff(
                F.current_date(), F.max(helper.timeColumn))
        )
            .alias("page_view_{pageType}_dur".format(
                pageType=item))) for item in pageTypes]

        self.assertEquals(
            [str(column) for column in helper.getAggregateColumnExpressions()], expectedAggregateColumns)


class PageViewFrequencyHelper_test(unittest.TestCase):
    def setUp(self) -> None:
        return SparkSession.builder.master("local[2]").appName("PageViewFrequencyHelper_test").getOrCreate()

    def test_must_be_subclass_of_AbstractAggregationHelper(self):
        object = PageViewFrequencyHelper([1], ["news"])
        self.assertIsInstance(object, AbstractAggregationHelper,
                              "PageViewFrequencyHelper must be inherited from AbstractAggregationHelper")

    def test_empty_pageTypes_should_fail(self):
        with self.assertRaises(Exception):
            PageViewFrequencyHelper([1], [])

    def test_empty_frequencies_should_fail(self):
        with self.assertRaises(Exception):
            PageViewFrequencyHelper([], ["news"])

    def test_expression_generation(self):
        pageTypes = ["news", "movies"]
        frequencies = [5, 10]
        selectColumns = []
        aggregateColumns = []
        for pageType in pageTypes:
            for frequency in frequencies:
                selectColumns.append("page_view_{page}_fre_{freq}".format(
                    page=pageType, freq=frequency))
                aggregateColumns.append(str(F.sum(
                    F.when(
                        (F.datediff(F.current_date(), F.col("VIEW_TIME")
                                    ).cast(IntegerType()) <= frequency) & (F.col("WEBPAGE_TYPE") == pageType), 1
                    ).otherwise(0)
                ).alias("page_view_{page}_fre_{freq}".format(
                    page=pageType, freq=frequency))))

        helper = PageViewFrequencyHelper(frequencies, pageTypes)
        helper.timeColumn = "VIEW_TIME"
        helper.categoryColumn = "WEBPAGE_TYPE"
        helper.generateExpressions()
        self.assertEquals(selectColumns, helper.getSelectColumnExpressions())
        self.assertEquals(aggregateColumns, [str(
            column) for column in helper.getAggregateColumnExpressions()])
