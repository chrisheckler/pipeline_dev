#!/usr/bin/env python3
# Copyright (c) 2018 Chris Heckler <hecklerchris@hotmail.com>

from pathlib import Path
import json
import os


# Stage 0
stage_0_json = Path(
    os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)), 'stage_0.json'))

stage_0_data = {'x':1,'y':1, 'z':1}

if not stage_0_json.exists():
    stage_0_json.write_text(json.dumps(stage_0_data))

print('Stage 0 data: ' + str(stage_0_data))


# Stage 1
stage_1_json = Path(
    os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)), 'stage_1.json'))

def add_value(my_dict, value):
    stage_1 = {k: v+value for k,v in my_dict.items()}
    return stage_1

stage_0_data = json.loads(stage_0_json.read_text())
stage_1_data = add_value(stage_0_data, 1)
stage_1_json.write_text(json.dumps(stage_1_data))

print('Stage 1 data: ' + str(stage_1_data))


# Stage 2
stage_2_json = Path(
    os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)), 'stage_2.json'))

def get_sum(my_dict):
    stage_2 = {'x': 0.3 * sum(my_dict.values()),
               'y': 0.6 * sum(my_dict.values()),
               'z': 0.9 * sum(my_dict.values())} 
    return stage_2

stage_1_data = json.loads(stage_1_json.read_text())
stage_2_data = get_sum(stage_1_data)
stage_2_json.write_text(json.dumps(stage_2_data))

print('Stage 2 data: ' + str(stage_2_data))
