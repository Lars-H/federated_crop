# Federated CROP - CSPF Estimation Planner

Federated CROP is an extension of [CROP](https://github.com/Lars-H/crop) to query federations of SPARQL endpoints.
The query planner uses estimated Characteristic Sets Profile Featues to obtain effiecient federated query plans. 
Federated CROP and CROP are based on the [Network of Linked Data Eddies](https://github.com/maribelacosta/nlde).

### Installation

The engine is implemented in Python 2.7.
It is advised to use a virtual environment for the installation in the following way:

```bash
# Install virtualenv
[sudo] python2 -m pip install virtualenv 

# Create a virtual environment
[sudo] python2 -m virtualenv venv

# Activate the environment
source venv/bin/activate

# Install the requirements
python -m pip install -r requirements.txt
```

Once the requirements are installed, the engine can be executed using the command line.

### Configuration

The CLI relies on a configuration YAML (see `config-example.yaml`) for the configuration of the SPARQL endpoints in the federation.
The configuration also needs to include the estimated CSPF files.
In this repository, we provide examples of those statistics files in th ``cspf`` directory.
Federated CROP uses two statistics files, one that maps sources to their predicates and one that maps sources to their characteristic sets.

### Usage

Before running the engine, make sure to correctly specify the federation in the configurration file.
Then queries can be executed as follows.

Example: 
```
venv/bin/python federated_crop.py -c config-example.yaml -f queries/fedbench_distinct/CD1.rq
```


### Help

You can find the help using:
```bash
venv/bin/python federated_crop.py -h
```
and get the usage options:
```bash
usage: federated_crop.py [-h] -c CONFIG -f QUERYFILE [-r {y,n}]
                         [-l {INFO,DEBUG}]

Federated CROP: Estimated Charset Query Planner for CROP

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Configuration YAML with federation, statistics and
                        additional parameter
  -f QUERYFILE, --queryfile QUERYFILE
                        file name of the SPARQL query (required, or -q)
  -r {y,n}, --printres {y,n}
                        format of the output results
  -l {INFO,DEBUG}, --log {INFO,DEBUG}
                        print logging information
```


## How to Cite

```
Lars Heling, Maribel Acosta. 
"Cost- and Robustness-based Query Optimization for Triple Pattern Fragment Clients" 
International Semantic Web Conference 2020.
```