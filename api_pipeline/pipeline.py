#!/usr/bin/env python3
import luigi
import json
#from pathlib import Path

class InputFile(luigi.Task):
    input_file = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget(self.input_file)

class StatusCodeCount(luigi.Task):
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

if __name__=="__main__":
    luigi.run()


