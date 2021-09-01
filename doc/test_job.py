#!/usr/bin/env python
# coding:utf-8

import sys
import os


def main():
    flag = 0
    with open(sys.argv[1]) as fi:
        for line in fi:
            if line.strip().startswith("name"):
                name = line.strip().split()[-1]
                print line.strip("\n")
            elif line.strip().startswith("cmd_begin"):
                flag = 1
                print line.strip("\n")
                continue
            elif line.strip().startswith("cmd_end"):
                print " "*8 + "echo %s" % name
                print line.strip("\n")
                flag = 0
            else:
                if flag:
                    continue
                print line.strip("\n")


if __name__ == "__main__":
    main()
