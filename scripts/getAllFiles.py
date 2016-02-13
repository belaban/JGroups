#!/usr/bin/env python
import os
from collections import defaultdict

hosts = {'mill001', 'mill004', 'mill006'}
user = 'a6915654'
file_location = '/work/Zab/'
#file_location = '/home/ryan/workspace/JGroups'
#file_location = '/home/pg/p11/a7109534/'
file_wildcard = '*'
extension = ".log"
get_file = file_location + file_wildcard + extension
destination = '.'
number_of_rounds = 18

#os.system("rm *" + extension)
for hostname in hosts:
    cmd = "scp " + user + "@" + hostname + ":" + get_file + " " + destination
    print cmd
    os.system(cmd)
