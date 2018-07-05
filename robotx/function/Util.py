import re
import os


class Unique(object):

    def __init__(self, prefix):
        self.prefix = prefix
        self.counter = 0

    def query(self):
        name = self.prefix + str(self.counter)
        self.counter += 1
        return name

NAME_MAPPING = dict()
SPARK_HOME = None

def function_naming(prefix):
    if prefix not in NAME_MAPPING:
        NAME_MAPPING[prefix] = Unique(prefix)
    return NAME_MAPPING[prefix].query()


def getDateFormat(date):
    # date: str
    dict_regex = {
        "[0-2][0-9]{3}": "yyyy",
        "[0-9]{4}-[0-9]{2}":"yyyy-MM",
        "[0-9]{4}-[0-9]{2}-[0-9]{2}": "yyyy-MM-dd",
        "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}": "yyyy-MM-dd HH",
        "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}": "yyyy-MM-dd HH:mm",
        "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}": "yyyy-MM-dd HH:mm:ss",
        "[0-2][0-9]{3}[0-1][0-9]": "yyyyMM",
        "[0-2][0-9]{3}[0-1][0-9][0-3][0-9]": "yyyyMMdd",
        "[0-2][0-9]{3}[0-1][0-9][0-3][0-9] [0-5][0-9]": "yyyyMMdd HH",
        "[0-2][0-9]{3}[0-1][0-9][0-3][0-9] [0-5][0-9]:[0-5][0-9]": "yyyyMMdd HH:mm",
        "[0-2][0-9]{3}[0-1][0-9][0-3][0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]": "yyyyMMdd HH:mm:ss",
        "[0-2][0-9]{3}/[0-1][0-9]":  "yyyy/MM",
        "[0-2][0-9]{3}/[0-1][0-9]/[0-3][0-9]": "yyyy/MM/dd",
        "[0-2][0-9]{3}/[0-1][0-9]/[0-3][0-9] [0-5][0-9]": "yyyy/MM/dd HH",
        "[0-2][0-9]{3}/[0-1][0-9]/[0-3][0-9] [0-5][0-9]:[0-5][0-9]": "yyyy/MM/dd HH:mm",
        "[0-2][0-9]{3}/[0-1][0-9]/[0-3][0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]": "yyyy/MM/dd HH:mm:ss"
    }
    for pattern in dict_regex.iterkeys():
        match_list = re.compile(pattern, flags=0).findall(date)
        if(len(match_list) == 0):
            continue
        else:
            match = match_list[0]
            if(len(match) == len(date)):
                return dict_regex[pattern]
    raise Exception("The format of input date " +date+ " is not recognized. ")

def getGradularity(dateFormat):
    # date:str. The input date format, e.g. yyyy-MM-dd, this is extracted from config.json.
    # return the format of date (str) and the granularity of the date (str)
    dict_date = {
        "yyyy": "year",
        "yyyy-MM": "month",
        "yyyy-MM-dd": "day",
        "yyyy-MM-dd hh": "hour",
        "yyyy-MM-dd hh:mm": "minute",
        "yyyy-MM-dd hh:mm:ss": "second",
        "yyyyMM": "month",
        "yyyyMMdd": "day",
        "yyyyMMdd hh": "hour",
        "yyyyMMdd hh:mm": "minute",
        "yyyyMMdd hh:mm:ss": "second",
        "yyyy/MM": "month",
        "yyyy/MM/dd": "day",
        "yyyy/MM/dd hh": "hour",
        "yyyy/MM/dd hh:mm": "minute",
        "yyyy/MM/dd hh:mm:ss": "second"
    }
    try:
        granularity = dict_date[dateFormat]
        return granularity
    except KeyError, e:
        raise Exception("The date format "+dateFormat+" is not supported. ")

def getMinGranulaty(granu_source, granu_target):
    granu_dict = {
        "year": 1,
        "month": 2,
        "day": 3,
        "hour": 4,
        "minute": 5,
        "second": 6
    }
    if not granu_dict.has_key(granu_source) or not granu_dict.has_key(granu_target):
        raise Exception("The granularity of source "+granu_source+" or target "+granu_target+" is not supported. ")
    if granu_dict[granu_source] <= granu_dict[granu_target]:
        return granu_source
    else:
        return granu_target


def sqlExp(s_date, t_date, sourceDate, targetDate):
    # s_date: str, date format of source, e.g. yyyy-MM-dd
    # t_date: str, date format of target.
    # sourceDate: str, the column name of date in source table
    # targetDate: str, the column name of date in target table
    # return the spark SQL expression (str) that calculate
    # the different between s_date and t_date in based on
    # the bigger timing granularity
    denomitor_dict = {
        "hour": 3600,
        "minute": 60,
        "second": 1
    }
    granu_source = getGradularity(s_date)
    granu_target = getGradularity(t_date)
    granu = getMinGranulaty(granu_source, granu_target)
    if granu in denomitor_dict.keys():
        denomitor = denomitor_dict[granu]
        sql_str = "round((UNIX_TIMESTAMP(`"+sourceDate+"`, '"+s_date+"')-UNIX_TIMESTAMP(`"+targetDate+"`, '"+t_date+"'))/"+str(denomitor)+")"
    elif(granu == "year"):
        sql_str = "year(FROM_UNIXTIME(UNIX_TIMESTAMP(`"+sourceDate+"`,'"+s_date+"')))-year(FROM_UNIXTIME(UNIX_TIMESTAMP(`"+targetDate+"`,'"+t_date+"')))"
    elif(granu == "month"):
        sql_str = "months_between(FROM_UNIXTIME(UNIX_TIMESTAMP(`"+sourceDate+"`,'"+s_date +"')),FROM_UNIXTIME(UNIX_TIMESTAMP(`"+targetDate+"`,'"+t_date+"')))"
    elif(granu == "day"):
        sql_str = "datediff(FROM_UNIXTIME(UNIX_TIMESTAMP(`"+sourceDate+"`,'"+s_date + "')),FROM_UNIXTIME(UNIX_TIMESTAMP(`"+targetDate+"`,'"+t_date+"')))"
    else:
        raise Exception("granularity " + granu + " is not supported. ")
    return sql_str

def unix_abs_path(path, pre_path=''):
    if not os.path.isabs(path):
        path = os.path.join(pre_path, path)
        path = os.path.abspath(path)

    path_sep = re.split("[\\\\]", path)
    path_rec = list()
    for pp in path_sep:
        pp = pp.strip()
        if pp == '':
            continue
        path_rec.append(pp)
    return "/".join(path_rec)