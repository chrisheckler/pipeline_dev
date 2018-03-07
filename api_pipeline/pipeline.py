#!/usr/bin/env python3
# Copyright (c) 2018 Chris Heckler <hecklerchris@hotmail.com>

import luigi
import json
from luigi.contrib.esindex import CopyToIndex


class InputFile(luigi.Task):
"""
This class takes a json file and passes it forward
to the next task
"""
    input_file = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget(self.input_file)

class StatusCodeCount(luigi.Task):
"""
This class parses the previous json file and collects
the code count for each status
"""
    input_file = luigi.Parameter()
    def requires(self):
        return InputFile(self.input_file)

    def output(self):
        return luigi.LocalTarget('aggregate_api.json')

    def run(self):
        data = {}
        for line in self.input().open().read().strip().split("\n"):
            j_line = json.loads(line)['_source']
            if j_line.get('api_key', None) and j_line.get('status', None):
                if data.get(j_line.get('api_key'), None):
                    if data[j_line.get('api_key')]['status_codes'].get(j_line.get('status')):
                        data[j_line.get('api_key')]['status_codes'][j_line.get('status')] += 1
                    else:
                        data[j_line.get('api_key')]['status_codes'].setdefault(j_line.get('status'), 1)
                else:
                    data.setdefault(j_line.get('api_key'), {'status_codes': {j_line.get('status'): 1}})

        with self.output().open('w') as fout:
            fout.write(json.dumps(data))

class ExampleIndex(CopyToIndex):
"""
This class takes the code counts and indexes into 
elasticsearch index api2
"""   
    input_file = luigi.Parameter()
    host = '192.168.254.122'
    port = 9200
    index = 'api2'
    doc_type = 'default'
    purge_existing_index = True
    marker_index_hist_size = 1

    def requires(self):
        return StatusCodeCount(self.input_file)

if __name__=="__main__":
    luigi.run()


