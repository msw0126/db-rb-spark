# -*- coding:utf-8 -*-
from pyspark import SQLContext, SparkConf, SparkContext, SparkFiles


def path(sc, filepath):
    """
    创建hadoop path对象
    :param sc sparkContext对象
    :param filename 文件绝对路径
    :return org.apache.hadoop.fs.Path对象
    """
    path_class = sc._gateway.jvm.org.apache.hadoop.fs.Path
    path_obj = path_class(filepath)
    return path_obj


def get_file_system(sc):
    """
    创建FileSystem
    :param sc SparkContext
    :return FileSystem对象
    """
    filesystem_class = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    hadoop_configuration = sc._jsc.hadoopConfiguration()
    return filesystem_class.get(hadoop_configuration)


def write(sc, filepath, content, overwite=True):
    """
    写内容到hdfs文件
    :param sc SparkContext
    :param filepath 绝对路径
    :param content 文件内容
    :param overwrite 是否覆盖
    """
    try:
        filesystem = get_file_system(sc)
        out = filesystem.create(path(sc, filepath), overwite)
        out.write(bytearray(content, "utf-8"))
        out.flush()
        out.close()
    except Exception as e:
        raise e
