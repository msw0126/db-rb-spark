# -*- coding:utf-8 -*-
import csv
from pyspark import SQLContext


def load_csv(spark, table):
    """
    :param spark: spark session
    :param table: table object which contains path and field describe
    :return: data frame in spark
    """
    reader = csv.reader(open(table.path, "r"), delimiter=table.delimiter)
    un_order_header = dict()
    for field_name in table.all_fields:
        if field_name not in table:
            un_order_header[field_name] = None
            continue
        field = table[field_name]
        if field.field_type == 'numeric':
            un_order_header[field_name] = float
        else:
            un_order_header[field_name] = None

    header = []
    col_type = []
    for row in reader:
        for r in row:
            if r not in un_order_header:
                raise Exception("column %s not found in configuration" % r)
            header.append(r)
            col_type.append(un_order_header[r])
        break
    col_num = len(header)

    i = 1
    data = list()
    for row in reader:
        if len(row) != col_num:
            raise Exception(
                "data not consist with header:line %d, expect %d columns, found %d" % (i, col_num, len(row)))
        line = list()
        for r, nm, tp in zip(row, header, col_type):
            if tp is None:
                line.append(r)
            else:
                try:
                    r = r.strip()
                    if r == '':
                        line.append(None)
                    else:
                        line.append(tp(r))
                except Exception as e:
                    raise Exception("line %d, column %s can not convert to float: %s" % (i, nm, r))
        data.append(tuple(line))
        i += 1
    rdd = spark.sparkContext.parallelize(data)
    data = SQLContext(spark.sparkContext).createDataFrame(rdd, header)
    print "%s loaded!" % table.name
    print data
    data.show()
    return data
