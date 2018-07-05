# -*- coding:utf-8 -*-
from robotx.RobotX import RobotX
from sys import argv

config_path, output_path, output_type, output_dict, dict_only = argv[1:]
# 如果dict_only="Y"，则dict_only=True,此时是False
dict_only = "Y" == dict_only

robotx = RobotX(config_path, output_path, output_type, output_dict, "RobotXSpark", dict_only)
robotx.run()
