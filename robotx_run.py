from robotx.RobotX import RobotX
from sys import argv

config_path, output_path, output_type, output_dict, dict_only = argv[1:]
dict_only = "Y"==dict_only

robotx = RobotX(config_path, output_path, output_type, output_dict, "RobotXSpark", dict_only)
robotx.run()
