import os


class SparkParser(object):

    accept_keys = {"spark.driver.cores", "spark.driver.maxResultSize", "spark.driver.memory", "spark.executor.memory",
                   "spark.logConf", "spark.master", "spark.submit.deployMode"}

    def __init__(self, path):
        self.path = path

    @property
    def config(self):
        configs = dict()
        if not os.path.exists(self.path):
            print "warning:spark.conf file not exist:"+self.path
            return configs, "local[*]"
        with open(self.path,"r") as f:
            line = f.readline()
            while(line):
                line = line.strip()
                if line.startswith("#") or len(line.split("="))==1:
                    line = f.readline()
                    continue
                kvp = line.split("=")
                key = kvp[0].strip()
                value = kvp[1].strip()
                configs[key] = value
                line = f.readline()
        config_copy = dict()
        master = "local[*]" if "master" not in configs else configs["master"]
        if "master" in configs:
            del configs["master"]

        print "accepted configs:"
        print self.accept_keys
        for key,value in configs.items():
            if key not in self.accept_keys:
                print "warning:%s not an spark config,ignored"
                continue
            config_copy[key] = value
        return config_copy, master