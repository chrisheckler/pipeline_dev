#!/usr/bin/env python3
import luigi
import json

class Stage0(luigi.Task):
    input_file = luigi.Parameter()
#    def requires(self):
#        return  

    def output(self):
        return luigi.LocalTarget(self.input_file)

#    def run(self):
#        return ... 

class Stage1(luigi.Task):
    input_file = luigi.Parameter()
#    def requires(self):
#        return  

    def output(self):
        return luigi.LocalTarget(self.input_file)

#    def run(self):
#        return ... 

class Stage2(luigi.Task):
    input_file = luigi.Parameter()
#    def requires(self):
#        return  

    def output(self):
        return luigi.LocalTarget(self.input_file)

#    def run(self):
#        return ... 

class Stage3(luigi.Task):
    input_file = luigi.Parameter()
#    def requires(self):
#        return  

    def output(self):
        return luigi.LocalTarget(self.input_file)

#    def run(self):
#        return ... 

class Stage4(luigi.Task):
    input_file = luigi.Parameter()
#    def requires(self):
#        return  

    def output(self):
        return luigi.LocalTarget(self.input_file)

#    def run(self):
#        return ... 

if __name__=="__main__":
    luigi.run()


