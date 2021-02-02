from nlde.query.sparql_parser import parse
from crop.query_plan_optimizer.charset_optimizer import Charset_Optimizer
import signal
from time import time
import logging
import sys
from nlde.engine.eddynetwork import EddyNetwork
import yaml
import os
import argparse


def get_options():

    parser = argparse.ArgumentParser(description="Federated CROP: Estimated Charset Query Planner for CROP")

    # nLDE arguments.
    parser.add_argument("-c", "--config",
                        help="Configuration YAML with federation, statistics and additional parameter", required=True)
    parser.add_argument("-f", "--queryfile",
                        help="file name of the SPARQL query (required, or -q)", required=True)
    parser.add_argument("-r", "--printres",
                        help="format of the output results",
                        choices=["y", "n"],
                        default="y")
    parser.add_argument("-l", "--log",
                        help="print logging information",
                        choices=["INFO", "DEBUG"])

    args = parser.parse_args()
    return args


def get_request_count(query_id):
    from ast import literal_eval
    request = 0
    interface2elapsed = {}
    all_elapsed = []
    ntt = 0
    logger = logging.getLogger("nlde_logger")
    if logger:
        with open('logs/{}.log'.format(query_id), "r") as infile:
            try:
                for line in infile.readlines():
                    if "request" in line:
                        dct = literal_eval(line)
                        request += dct['requests']
                    if "elapsed" in line:
                        dct = literal_eval(line)
                        elapsed = float(dct['elapsed'])
                        all_elapsed.append(elapsed)
                        interface = dct['interface']
                        interface2elapsed[interface] = interface2elapsed.get(interface, 0) + elapsed
                    if "tuples" in line:
                        dct = literal_eval(line)
                        ntt += int(dct['tuples'])
                return request, sum(all_elapsed), len(all_elapsed), ntt
            except Exception as e:
                print line
                print e
                return -1, -1 , -1, -1
    return -1, -1, -1, -1


def execute_crop(**kwargs):

    print_results = True if kwargs.get("printres") == "y" else False

    logger = logging.getLogger("nlde_logger")
    logger_debug = logging.getLogger("nlde_debug")
    log_level = kwargs.get("log")

    if log_level == "INFO":
        logger.setLevel(logging.INFO)
        logger_debug.setLevel(logging.ERROR)
    elif log_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
    else:
        logger_debug.setLevel(logging.ERROR)
        logger.setLevel(logging.ERROR)

    # Load Config File
    config_fn= kwargs.get("config")
    config_dict = yaml.load(open(config_fn), Loader=yaml.FullLoader)
    source2uri =  config_dict['federation']
    timeout = int(config_dict['timeout'])
    p2sourcesfn =  config_dict['statistics']['predicates']
    s2csourcesfn =  config_dict['statistics']['characteristic_sets']


    # Set up optimizer (just once)
    optimizer = Charset_Optimizer(sources=source2uri.values(), p2sources=p2sourcesfn, sources2cs=s2csourcesfn,
                                  dataset2uri=source2uri)

    query_fn = kwargs.get("queryfile")
    query_id = query_fn.split("/")[-1].replace(".rq", "")

    logger.info('START %s', query_id)
    fhandler = logging.FileHandler('logs/{}.log'.format(query_id), 'w')
    fhandler.setLevel(logging.INFO)
    logger.addHandler(fhandler)

    try:
        query = parse(open(query_fn).read())
        t0 = time()
        plan = optimizer.create_plan(query)
        planning_time = time() - t0
        #print(plan)
        logger.info(plan)
        en = EddyNetwork()

        if timeout:
            signal.signal(signal.SIGALRM, en.stop_execution)
            signal.alarm(timeout)

        result_count = 0
        t0 = time()
        for result in en.execute_standalone(plan):
            if print_results:
                print result
            result_count += 1

        tdelta = time() - t0

        request_cnt, requests_elapsed_sum, requests_elapsed_cnt, ntt = get_request_count(query_id)

        selected_source = len(optimizer.select_sources)
        distinct_selected_source = len(set(optimizer.select_sources))

        print "\t".join(["{}".format(str(elem)) for elem in [query_id, planning_time, tdelta, planning_time + tdelta,
                                                     request_cnt, result_count, p2sourcesfn, \
            s2csourcesfn, optimizer.number_of_subqueries, selected_source, distinct_selected_source, ntt ,
                                                              plan]])
    except Exception as e:
        raise e



if __name__ == '__main__':

    options = get_options()
    execute_crop(**vars(options))

