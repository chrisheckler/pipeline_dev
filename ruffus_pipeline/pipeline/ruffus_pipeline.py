#!/usr/bin/env python3
# Copyright (c) 2018 Chris Heckler <hecklerchris@hotmail.com>

from ruffus import *
import csv

starting_file = ['data.csv']
 
#
# STAGE 1 - Read in file and multiply numbers by 100
#

@transform(starting_file,
            suffix(".csv"),
            ".csv")
def read_multiply(input_file, output_file):
    data = []

    with open(input_file) as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            for num in row:
                data.append(int(num))

    transformed = [x*100 for x in data]
    print(transformed)
    with open(output_file, 'w') as f:
        writer = csv.writer(f, delimiter=',')   
        for i in transformed:
            writer.writerow(str(i))
#
# STAGE 2 - Add 15 to the numbers
#

@transform(read_multiply,
            suffix(".csv"),
            ".csv")

def add_num(input_file, output_file):

    data = []
    with open(input_file) as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            for num in row:
                nums = int(num)
                data.append(nums)
    
    transformed = [x+15 for x in data]
    print(transformed)

    with open(output_file, 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for i in range(len(transformed)):
            writer.writerow(str(i))

    
pipeline_run()
