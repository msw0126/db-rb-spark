import copy

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StringType, StructType


class FactorLevelLimitation:

    def __init__(self, max_level=200, obtain_max_level=20, obtain_percent=0.85):
        self.max_level = max_level
        self.obtain_max_level = obtain_max_level
        self.percent = obtain_percent

    def execute(self, data, fac, n):
        """
        :param fac: factor name
        :param n: num of rows
        :param data: DataFrame
        :type data: DataFrame
        :return:
        """
        percent_n = self.percent*n

        group_fac_data = data.groupBy(fac)
        fac_count = group_fac_data.count()
        fac_level = fac_count.count()
        if fac_level > self.max_level:
            # max level allowed, skip
            print("skip factor [%s] max level exceed, %d, %d allowed"
                  %(fac, fac_level, self.max_level))
            return None

        fac_count = [(r[fac],r['count']) for r in fac_count.collect()]
        fac_count.sort(key=lambda x: x[1], reverse=True)

        levels_obtain = list()
        cum_sum = 0
        for count, (k, v) in enumerate(fac_count):
            cum_sum += v
            if cum_sum > percent_n:
                levels_obtain.append(k)
                if count < fac_level-1:
                    levels_obtain.append(copy.deepcopy(levels_obtain))
                print("obtain factor [%s], %.2f%% reached" %(fac, self.percent))
                return levels_obtain
            if count >= self.obtain_max_level:
                if count < fac_level-1:
                    levels_obtain.append(copy.deepcopy(levels_obtain))
                print("obtain factor [%s], level %d reached" % (fac, self.obtain_max_level))
                return levels_obtain
            levels_obtain.append(k)
        print("obtain factor [%s]")
        return levels_obtain

    def __str__(self):
        return "level<=%d--->obtain[%d or %.2f%%]" %(self.max_level, self.obtain_max_level, self.percent)


if __name__ == "__main__":
    sc = SparkSession \
        .builder \
        .getOrCreate()

    data_schema_list = list()
    field_schema0 = StructField('aa', StringType())
    field_schema1 = StructField('bb', StringType())
    field_schema2 = StructField('cc', StringType())
    data_schema_list.append(field_schema0)
    data_schema_list.append(field_schema1)
    data_schema_list.append(field_schema2)

    data_schema = StructType(data_schema_list)
    reader = sc.read
    data = reader.csv(path="tt", sep=",", schema=data_schema, header=False, mode='FAILFAST')
    n = data.count()

    f = FactorLevelLimitation()
    for fac in ['aa','bb','cc']:
        ld = f.execute(data,fac, n)
        print(fac, ld)

