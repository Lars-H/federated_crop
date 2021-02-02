from nlde.query.sparql_parser import parse
from crop.query_plan_optimizer.charset_optimizer import Charset_Optimizer
import signal
from time import time
import logging
#logging.getLogger("nlde_logger").setLevel(logging.WARNING)
import sys
from nlde.engine.eddynetwork import EddyNetwork


logger = logging.getLogger("nlde_logger")
logger.setLevel(logging.INFO)


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


if __name__ == '__main__':
    #dataset_names = ["dbpedia", "nytimes", "geonames", "linkedmdb", "kegg", "chebi", "drugbank", "swdf", "jamendo"]
    #sources = ["http://aifb-ls3-vm8.aifb.kit.edu:5000/" + dataset_name for dataset_name in dataset_names]

    timeout = 120

    source2uri = {
        "dbpedia" : "sparql@http://aifb-ls3-remus.aifb.kit.edu:8890/sparql",
        "drugbank" : "sparql@http://aifb-ls3-remus.aifb.kit.edu:8891/sparql",
        "kegg" : "sparql@http://aifb-ls3-remus.aifb.kit.edu:8892/sparql",
        "chebi" : "sparql@http://aifb-ls3-remus.aifb.kit.edu:8893/sparql",
        "geonames" : "sparql@http://aifb-ls3-remus.aifb.kit.edu:8894/sparql",
        "jamendo" : "sparql@http://aifb-ls3-remus.aifb.kit.edu:8895/sparql",
        "nytimes" : "sparql@http://aifb-ls3-remus.aifb.kit.edu:8896/sparql",
        "swdf" : "sparql@http://aifb-ls3-remus.aifb.kit.edu:8897/sparql",
        "linkedmdb" : "sparql@http://aifb-ls3-remus.aifb.kit.edu:8898/sparql"
    }

    p2sourcesfn = "stats/cs_dicts/p2s-unweighted-0_0001-1.json"
    s2csourcesfn = "stats/cs_dicts/s2cs-unweighted-0_0001-1.json"
    #p2sourcesfn = "stats/cs_dicts/p2s-complete_top_10000.json"
    #s2csourcesfn = "stats/cs_dicts/s2cs-complete_top_10000.json"
    cds = ["queries/fedbench_distinct/CD{}.rq".format(i) for i in range(1, 8)]
    lds = ["queries/fedbench_distinct/LD{}.rq".format(i) for i in range(1, 12)]
    lss = ["queries/fedbench_distinct/LS{}.rq".format(i) for i in range(1, 8)]
    query_fns = cds + lds + lss
    query_fns = ["queries/fedbench_distinct/CD4.rq"]

    if len(sys.argv) == 4:
        query_fns = [sys.argv[1]]
        p2sourcesfn = sys.argv[2]
        s2csourcesfn = sys.argv[3]
    elif len(sys.argv) == 3:
        p2sourcesfn = sys.argv[1]
        s2csourcesfn = sys.argv[2]

    # Set up optimizer (just once)
    optimizer = Charset_Optimizer(sources=source2uri.values(), p2sources=p2sourcesfn, sources2cs=s2csourcesfn,
                                  dataset2uri=source2uri, merge_eg=False)

    ##logging.getLogger("nlde_logger").setLevel(logging.WARNING)
    for query_fn in query_fns:
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
            sys.exit()
            en = EddyNetwork()

            if timeout:
                signal.signal(signal.SIGALRM, en.stop_execution)
                signal.alarm(timeout)

            result_count = 0
            t0 = time()
            for result in en.execute_standalone(plan):
                result_count += 1

            tdelta = time() - t0

            request_cnt, requests_elapsed_sum, requests_elapsed_cnt, ntt = get_request_count(query_id)

            selected_source = len(optimizer.select_sources)
            distinct_selected_source = len(set(optimizer.select_sources))

            print "\t".join(["{}".format(str(elem)) for elem in [query_id, planning_time, tdelta, planning_time + tdelta,
                                                         request_cnt, result_count, p2sourcesfn, \
                s2csourcesfn, optimizer.number_of_subqueries, selected_source, distinct_selected_source, ntt ,
                                                                 optimizer.merge_excl_grps, plan]])
        except Exception as e:
            print "{}, {} {}: {}".format(query_id, p2sourcesfn, s2csourcesfn, str(e))