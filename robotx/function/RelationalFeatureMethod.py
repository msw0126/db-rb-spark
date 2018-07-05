from robotx.bean.Beans import *
from pyspark.sql import DataFrame,functions
from abc import ABCMeta, abstractmethod
import robotx.bean.Constant as C
from pyspark import SQLContext
from pyspark.sql.types import *


class SummaryMethod(object):

    __metaclass__ = ABCMeta

    SUMMARY = None
    ONE_HOT_EXCEPTED = False

    def __init__(self, table, field, new_table, interval=None):
        self.table = table
        self.field = field
        self.new_table = new_table
        self.new_field = None
        self.interval = interval

    def create_field(self):
        new_field_name = "{0}<{1}>".format(self.SUMMARY, self.field.name)
        if self.interval is not None:
            new_field_name = "{0}{2}<{1}>".format(self.SUMMARY, self.field.name, self.interval)
        new_field = NumericField(new_field_name, describer=SummaryDescriber(self.field, self.SUMMARY,self.interval))
        self.new_field = new_field
        self.new_table+new_field

    @abstractmethod
    def __method__(self): pass

    def method(self):
        self.create_field()
        return self.__method__().alias(self.new_field.name)


class Avg(SummaryMethod):

    SUMMARY = "avg"

    def __method__(self):
        field = self.field
        return functions.avg(col(field.name))


class Sum(SummaryMethod):

    SUMMARY = "sum"

    def __method__(self):
        field = self.field
        return functions.sum(col(field.name))


class Max(SummaryMethod):

    SUMMARY = "max"
    ONE_HOT_EXCEPTED = True

    def __method__(self):
        field = self.field
        return functions.max(col(field.name))


class Min(SummaryMethod):

    SUMMARY = "min"
    ONE_HOT_EXCEPTED = True

    def __method__(self):
        field = self.field
        return functions.min(col(field.name))


class SummaryDescriber(object):

    def __init__(self, field, summary, interval=None):
        self.field = field
        self.summary = summary
        self.interval = None

    @property
    def expression(self):
        field = self.field.expression
        summary = self.summary
        return "{smr}<{f}>".format(smr=summary,f=field)


class TimeSeriesMethod(object):

    REGISTER_DICT = set()

    METHOD = None
    __metaclass__ = ABCMeta

    def __init__(self, table, field, interval, period=None):
        self.table = table
        self.interval = interval
        self.field = field
        self.period = period

    def method(self, spark):
        """
        :type spark: SparkSession
        :return: 
        """
        function_name = self.__function_name__()
        if function_name not in self.REGISTER_DICT:
            function_object = self.__method__()
            spark.udf.register(function_name,function_object, DoubleType())
            self.REGISTER_DICT.add(function_name)

        table = self.table
        field = self.field
        new_field_name = "%s<%s>" %(function_name,field.name)
        new_field = NumericField(new_field_name, describer=TimeSeriesDescriber(field, self.METHOD, self.interval, self.period))
        table + new_field
        return "{function_name}(`{date_diff}`,`{value_field}`) as `{new_name}`".format(
            function_name = function_name,
            date_diff = C.__DATE_DIFF_LIST__,
            value_field = field.name+C.__LIST_APPENDER__,
            new_name = new_field_name
        )

    @abstractmethod
    def __method__(self): pass

    @abstractmethod
    def __function_name__(self): pass

class Wma(TimeSeriesMethod):

    METHOD = "wma"

    def __method__(self):
        """
        period = 3
        d v
        1 10     --->  vector a : [10,30,4,6]
        2 30     --->  vector b : [1/1,1/2,1/3,1/4]
        3 4      --->  a*b/sum(b)
        4 6
        """
        def tmp_method(d,v):
            denorm = 0.0
            numer = 0.0
            for d_, v_ in zip(d, v):
                denorm += (1.0 / d_) * v_
                numer += (1.0 / d_)
            if numer==0:
                return None
            if denorm==0:
                return 0.0
            return denorm / numer
        return tmp_method

    def __function_name__(self):
        return self.METHOD


class Wdiff(TimeSeriesMethod):

    METHOD = 'wdiff'

    def __method__(self):
        """
        period = 3
        d v
        1 10     --->  vector a : [10-30,30-4,4-6]
        2 30     --->  vector b : [1/1,1/2,1/3]
        3 4      --->  a*b/sum(b)
        4 6
        """
        period = self.period
        def tmp_method(d,v):
            # todo better ways?
            t = sorted(zip(d, v), key=lambda x: x[0])
            denorm = 0.0
            numer = 0.0
            pre_d = None
            pre_v = None
            for d,v in t:
                if pre_d==None:
                    pre_d = d
                    pre_v = v
                else:
                    if d-pre_d==1:
                        denorm += (pre_v-v)*1.0/pre_d
                        numer += 1.0/pre_d
                    pre_d = d
                    pre_v = v
            if numer==0:
                return None
            if denorm==0:
                return 0.0
            return denorm/numer
        return tmp_method

    def __function_name__(self):
        return self.METHOD


class Recent(TimeSeriesMethod):

    def __method__(self):
        """
        period = 3
        d v
        1 10     --->  10+30+4
        2 30
        3 4
        4 6
        """
        period = self.period
        def tmp_method(d,v):
            num = 0.0
            for d_, v_ in zip(d, v):
                if d_<=period:
                    num += v_
            return num
        return tmp_method

    METHOD = "rec"

    def __function_name__(self):
        period = self.period
        return self.METHOD+str(period)


class RecentDivideMean(TimeSeriesMethod):

    METHOD = "rdm"

    def __method__(self):
        """
        d v
        1 10     --->  10/((30+4)/2)
        2 30
        3 4
        """
        def tmp_method(d,v):
            denorm = 0.0
            numer = 0.0
            for d_, v_ in zip(d, v):
                if d_==1:
                    denorm = v_
                else:
                    numer += v_
            if numer==0:
                return None
            if denorm==0:
                return 0.0
            return denorm*(len(d)-1)*1.0/numer
        return tmp_method

    def __function_name__(self):
        return self.METHOD


class TimeSeriesDescriber(object):

    def __init__(self, field, method, interval, period=None):
        self.field = field
        self.interval = interval
        self.method = method
        self.period = period

    @property
    def expression(self):
        field = self.field.expression
        if self.period is None:
            return "{mtd}({f},{itv})".format(mtd=self.method, f=field, itv=self.interval)
        return "{mtd}({f},{p},{itv})".format(mtd=self.method, f=field, p=self.period, itv=self.interval)