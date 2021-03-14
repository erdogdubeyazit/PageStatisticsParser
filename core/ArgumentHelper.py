import argparse


class PageViewStaticticsApplicationInputParameterParser:
    """
    Parses and validates environmental variables required for PageViewStaticticsApplication.

    """

    def __init__(self):
        """

        Fields
        ------
        * pagetype          : Comma seperated 'WEBPAGE_TYPE' values in 'lookup' data.
        * metrictype        : Requested statistics. Use 'fre' for time windowed frequencies, 'dur' for recency. Indicators must be comma seperated.
        * timewindow        : Comma seperated integers for time windowing.
        * dateofreference   : Date string to caculate how many days have passed since the last activity after this date reference.
       
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--pagetype", help="Comma seperated 'WEBPAGE_TYPE' values in 'lookup' data.")
        parser.add_argument(
            "--metrictype", help="Please indicate requested statistics. Use 'fre' for time windowed frequencies, 'dur' for recency. Indicators must be comma seperated.")
        parser.add_argument(
            "--timewindow", help="Comma seperated integers for time windowing.")
        parser.add_argument(
            "--dateofreference", help="Please indicate a date string to caculate how many days have passed since the last activity after this date reference.")

        args = parser.parse_args()

        if(args.pagetype):
            pageTypes = args.pagetype.split(",")
            if(pageTypes is None or len(pageTypes) == 0 or isinstance(pageTypes, list) == False or all(isinstance(item, str) for item in pageTypes) == False):
                raise Exception("The parameter pagetype is invalid.")
            self.pageTypes = [item.strip() for item in pageTypes]
        else:
            raise Exception("The parameter pagetype did not indicated.")

        if(args.metrictype):
            metrics = args.metrictype.split(",")
            if(metrics is None or len(metrics) == 0 or isinstance(metrics, list) == False or all(isinstance(item, str) for item in metrics) == False):
                raise Exception("The parameter metrictype is invalid.")
            metrics = set([item.strip() for item in metrics])
            self.isDuration = True if "dur" in metrics else False
            self.isFrequency = True if "fre" in metrics else False
            if((self.isDuration or self.isFrequency) == False):
                raise Exception("Metric types are invalid.")
        else:
            raise Exception("The parameter metrictype did not indicated.")

        if(args.timewindow):
            dates = args.timewindow.split(",")
            if(dates is None or len(dates) == 0 or isinstance(dates, list) == False):
                raise Exception("The parameter timewindow is invalid")
            self.timeWindows = [int(item.strip()) for item in dates]
        else:
            raise Exception("The parameter metrictype did not indicated.")

        if(args.dateofreference):
            self.timeOfReference = args.dateofreference.strip()
        else:
            raise Exception("The parameter dateofreference did not indicated.")

    def getPageTypes(self):
        return self.pageTypes

    def isDurationRequested(self):
        return self.isDuration

    def isFrequencyRequested(self):
        return self.isFrequency

    def getTimeWindows(self):
        return self.timeWindows

    def getTimeOfReference(self):
        return self.timeOfReference
