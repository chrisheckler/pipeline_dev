# api_pipeline
This is a pipeline done with the luigi pipeline software
I will take a json file and output a dictionary containing api_key's and count of status codes.  Then I will take the previous output and index it into elasticsearch

## Stage0
A json file is read into the pipeline and passed onto Stage1

## Stage1
The json is parsed and the status code count for each api key is returned in json

## Stage2
The return status code count json file will then be indexed into elasticsearch
