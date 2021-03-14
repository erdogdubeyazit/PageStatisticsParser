from core.Template import ApplicationTemplate
from pyspark.sql.types import StringType, StructField, StructType
from PageViewStatisticsApplication import PageViewStaticticsApplication
import unittest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
from datetime import datetime, timedelta


class PageViewStatisticsApplication_test(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[2]").appName(
            "PageViewStatisticsApplication_test").getOrCreate()
        self.logger = logging.getLogger('py4j')

    def test_must_be_subclass_of_ApplicationTemplate(self):
        app = PageViewStaticticsApplication(self.spark, self.logger, [
            "news"], True, True, [1], "01/01/2000")
        self.assertIsInstance(app, ApplicationTemplate,
                              "PageViewStaticticsApplication must be inherited from ApplicationTemplate")

    def test_empty_parameters_should_fail(self):
        with self.assertRaises(Exception):
            PageViewStaticticsApplication(self.spark, self.logger).run()

    def test_empty_pagetypes_should_fail(self):
        with self.assertRaises(Exception):
            PageViewStaticticsApplication(
                self.spark, self.logger, [], True, True, [1], "01/01/2000").run()

    def test_DurationTrueAndDateOfReferenceNone_should_fail(self):
        with self.assertRaises(Exception):
            PageViewStaticticsApplication(self.spark, self.logger, [
                                          "news"], True, True, [1], None).run()

    def test_FrequencyTrueAndTimeWindowsEmpty_should_fail(self):
        with self.assertRaises(Exception):
            PageViewStaticticsApplication(self.spark, self.logger, [
                                          "news"], True, True, [], "01/01/2000").run()

    def test_dataFrames_without_common_WEB_PAGEID_column_shoul_fail(self):
        app = PageViewStaticticsApplication(self.spark, self.logger, [
            "news"], True, True, [1], "01/01/2000")
        schema = StructType([
            StructField('column_name', StringType(), True)
        ])
        df1 = self.spark.createDataFrame(
            self.spark.sparkContext.emptyRDD(), schema)
        df1.printSchema()
        df2 = self.spark.createDataFrame(
            self.spark.sparkContext.emptyRDD(), schema)
        df2.printSchema()
        with self.assertRaises(Exception):
            app.joinFactAndLookupData(df1, df2, "WEB_PAGEID")

    def test_handle_process(self):
        schema = StructType([
            StructField('USER_ID', StringType(), True),
            StructField('WEBPAGE_TYPE', StringType(), True),
            StructField('WEB_PAGEID', StringType(), True),
            StructField('VIEW_TIME', StringType(), True),
        ])

        now = datetime.now()
        two_days_ago = now-timedelta(days=2)
        five_days_ago = now-timedelta(days=5)
        six_days_ago = now-timedelta(days=6)

        data = []
        for userId in [1, 2]:
            for webPage in [(1111, 'news'), (2222, 'movies')]:
                for viewTime in [two_days_ago.strftime("%d/%m/%Y %H:%M"), five_days_ago.strftime("%d/%m/%Y %H:%M")]:
                    data.append((userId, webPage[1], webPage[0], viewTime))

        testDataFrame = self.spark.createDataFrame(data, schema).select("USER_ID", "WEBPAGE_TYPE", "WEB_PAGEID", F.to_date(
            "VIEW_TIME", "dd/MM/yyyy HH:mm").alias("VIEW_TIME"))

        app = PageViewStaticticsApplication(self.spark, self.logger, [
            "news", "movies"], True, True, [2, 5], six_days_ago.strftime("%d/%m/%Y"))
        app.dataFrame = testDataFrame
        app.handleProcess()

        self.assertEqual(2, app.dataFrame.count())

        self.assertEqual(
            1, app.dataFrame.filter(F.col("user_id") == 1).filter(F.col("page_view_news_dur") == 2).filter(
                F.col("page_view_news_fre_2") == 1).filter(F.col("page_view_news_fre_5") == 2).filter(F.col("page_view_movies_fre_2") == 1).filter(F.col("page_view_movies_fre_5") == 2).count()
        )

        self.assertEqual(
            1, app.dataFrame.filter(F.col("user_id") == 2).filter(F.col("page_view_news_dur") == 2).filter(
                F.col("page_view_news_fre_2") == 1).filter(F.col("page_view_news_fre_5") == 2).filter(F.col("page_view_movies_fre_2") == 1).filter(F.col("page_view_movies_fre_5") == 2).count()
        )


if __name__ == '__main__':
    unittest.main()
