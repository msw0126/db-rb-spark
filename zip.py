# -*- coding:utf-8 -*-
"""
代码打包
"""
import os, zipfile


def zip_dir(dir_list, zipfilename):
    filelist = []
    for dirname in dir_list:
        if os.path.isfile(dirname):
            filelist.append(dirname)
        else :
            for root, dirs, files in os.walk(dirname):
                for name in files:
                    filelist.append(os.path.join(root, name))
                    # print os.path.join(root, name)

    zf = zipfile.ZipFile(zipfilename, "w", zipfile.zlib.DEFLATED)
    for tar in filelist:
        arcname = tar
        print arcname
        zf.write(tar, arcname)
    zf.close()


if __name__ == '__main__':
    dir_list = ["./robotx"]
    zip_dir(dir_list, "./robotx.zip")
