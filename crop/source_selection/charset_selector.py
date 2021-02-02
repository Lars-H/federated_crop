from itertools import combinations
import json
from nlde.query import BGP
import logging
logger = logging.getLogger("nlde_debug")
logger.setLevel(logging.INFO)

def findsubsets(s, n):
    return list(map(set, combinations(s, n)))

def mean(series):
    return sum(series) / len(series)


class CharSet_Selector(object):

    def __init__(self, p2sources, sources2cs, dataset2uri, **kwargs ):

        self.p2sources_fn = p2sources
        self.source2cs_fn = sources2cs
        self.dataset2uri = dataset2uri

        self.sources = dataset2uri.values()
        self.load_stats(p2sources, sources2cs)


        # For tp: <s,?p,?o>
        # Max: Assume that at most one source can answer the
        self.bound_subject_fct = lambda x: max(x)

        self.uri2dataset = {}
        for dataset, uri in self.dataset2uri.items():
            self.uri2dataset[uri] = dataset

    @property
    def params(self):
        return {
            "p2sources_fn" : self.p2sources_fn,
            "source2cs_fn": self.source2cs_fn,
        }

    def load_stats(self, p2sources, sources2cs):

        self.source2min_predicate = {}
        with open(p2sources, "r") as infile:
            self.predicate2source = json.load(infile)

            for predicate, source_cnt in self.predicate2source.items():
                for source, cnt in source_cnt.items():
                    self.source2min_predicate[source] = min(cnt, self.source2min_predicate.get(source, float("inf")))

        self.dataset2cs = {}
        self.dataset2degrees = {}
        self.dataset2max_rel_multiplicity = {}
        with open(sources2cs, "r") as infile:
            tmp_dct = json.load(infile)
            for datasource, css in tmp_dct.items():
                self.dataset2cs[datasource] = {}
                self.dataset2degrees[datasource] = []

                multiplcities = []
                for cs, cnt in css.items():
                    triples = 0
                    cs_set = frozenset(str(cs).split(","))
                    cs_dct = cnt
                    self.dataset2cs[datasource][cs_set] = cs_dct
                    for p, cnt in cs_dct.items():
                        if p != "count":
                            triples += cnt
                            multiplcities.append(cnt/cs_dct['count'])
                    self.dataset2degrees[datasource].append(triples / cs_dct['count'])

                self.dataset2max_rel_multiplicity[datasource] = max(multiplcities)


    def estimate_cardinality(self, triple_patterns, charsets):

        # Single triple pattern case
        if len(triple_patterns) == 1 and len(charsets.keys()) == 0:

            card_dict = {}
            var_position = triple_patterns[0].variable_position
            # Case: <s,p,?o>, <?s,p,o> , <?s,p,?o>
            if var_position in [1, 4, 5]:
                predicate = triple_patterns[0][1][1:-1]

                relevant_sources = self.predicate2source.get(predicate, [])

                # We do find relevant sources
                if len(relevant_sources) > 0:
                    for source, cnt in relevant_sources.items():
                        source_uri = self.dataset2uri[source]
                        triple_patterns[0].sources[source_uri] = cnt
                        card_dict[source_uri] = cnt

                    triple_patterns[0].estimation_case = 1
                    return card_dict #sum(relevant_sources.values())

                # We do not find relvant sources
                # If we don't find a relevant source, we could assume that the estimated cardinality per source
                # is given by the minimum cardinality of all predicates in that source
                # Idea: we assume that if we didn't sample the predicate it must occur less frequently than the
                # least frequent in our sample
                else:
                    for dataset, uri in self.dataset2uri.items():
                        if var_position == 4:
                            # We assume that if object is bound, we have the maximum relative multiplicity as the
                            # number of subjects
                            b = self.dataset2max_rel_multiplicity[dataset]
                            est_card = b
                            triple_patterns[0].estimation_case = 2
                        else:
                            est_card = self.source2min_predicate[dataset]
                            triple_patterns[0].estimation_case = 3
                        triple_patterns[0].sources[uri] = est_card
                        card_dict[uri] = est_card
                        #card += est_card
                    return card_dict #card

            # Case: <s,?p,?o>
            elif var_position == 3:
                means = []
                for source in self.sources:
                    dataset = self.uri2dataset[source]
                    degrees = self.dataset2degrees[dataset]
                    mean_deg = mean(degrees)
                    triple_patterns[0].sources[source] = mean_deg
                    means.append(mean_deg)
                    card_dict[source] = mean_deg

                triple_patterns[0].estimation_case = 4
                return card_dict #self.bound_subject_fct(means)

            # Case: <?s,?p,?o>
            elif var_position == 7:
                triple_patterns[0].estimation_case = 5
                for uri, dataset in self.uri2dataset.items():
                    card_dict[uri] = float("inf")
                return card_dict #float("inf")

            # All other cases
            else:
                triple_patterns[0].estimation_case = 7
                for uri, dataset in self.uri2dataset.items():
                    card_dict[uri] = 1
                return card_dict #float("inf")


        # Base Case (Neumann)
        card_dict = {}
        for source_uri, source_charsets in charsets.items():
            card = 0.0
            for charset in source_charsets:
                m = 1.0
                o = 1.0
                for tp in triple_patterns:
                    tp.estimation_case = 6
                    # Predicate is variable
                    if tp.variable_position in [2,3,6,7]: continue

                    predicate = tp[1][1:-1]
                    if tp.variable_position == 4:
                        mult_p_in_cs = charset[predicate]
                        o = min(o, 1.0 / mult_p_in_cs)
                    else:
                        m = m * (charset[predicate] / charset['count'])
                card = card + charset['count'] * m * o
            card_dict[source_uri] = card

        return card_dict

    def charset2source(self, charset, sources):

        # If there is a variable in the predicate position
        to_remove = set()
        for elem in charset:
            if str(elem)[0] == "?":
                to_remove.add(elem)
        charset = charset - to_remove
        if len(charset) == 0:
            return self.sources, {}



        rel_sources = {}
        charsets = {}
        for source in sources:
            for source_cs, cnts in self.dataset2cs.get(source, {}).items():
                if charset.issubset(source_cs):
                    source_uri = self.dataset2uri[source]
                    rel_sources[source_uri] = rel_sources.get(source_uri, 0)  + cnts['count']
                    #charsets.append((cnts, source_uri))
                    charsets.setdefault(source_uri, []).append(cnts)

        # Base case for testing subsets
        if len(charset) == 1 and len(rel_sources.keys()) == 0:
            # All sources are relevant
            for uri in self.dataset2uri.values():
                rel_sources[uri] = 1
            return rel_sources, {}

        return rel_sources, charsets


    def execute_source_selection(self, star):

        predicates2tp, rel_sources = self.set_source(star)
        predicates = predicates2tp.keys()

        elems = []
        todo = set(predicates)

        if len(predicates) == 1:
            relevant_sources, charsets = self.charset2source(set(predicates), rel_sources)
            tp = predicates2tp.values()[0][0]
            card_dict = self.estimate_cardinality([tp], charsets)
            est_card = sum(card_dict.values())
            tp.cardinality = est_card
            return [tp]

        for i in reversed(range(1,len(predicates))):
            subsets = findsubsets(predicates, i+1)

            for subset in subsets:
                logger.debug("Testing subset: {}".format(subset))
                relevant_sources, charsets = self.charset2source(subset, rel_sources)

                if len(relevant_sources.keys()) > 0:
                    tps = []
                    for pred in subset:
                        for tp in predicates2tp[pred]:
                            tp.sources = relevant_sources
                            tps.append(tp)

                    # Estimate cardinality
                    card_dict = self.estimate_cardinality(tps, charsets)
                    if len(tps) > 1:
                        bgp = BGP(tps)
                        for tp in tps:
                            # Set exptected cardinalities according to estimations per source
                            tp.sources = card_dict
                    else:
                        bgp = tps[0]

                    bgp.cardinality = sum(card_dict.values())  # sum(relevant_sources.values())
                    bgp.estimation_case = 6
                    elems.append(bgp)
                    logger.debug("found bgp for subset: {} of size {}".format(subset, i+1))
                    # Remove from todo
                    todo = todo - subset

                    break
            else:
                continue
            break

        for predicate in todo:
            for tp in predicates2tp[predicate]:
                relevant_sources, charsets = self.charset2source(set(predicates), rel_sources)
                card_dict = self.estimate_cardinality([tp], charsets)
                tp.cardinality = sum(card_dict.values())
                elems.append(tp)

        return elems

    def set_source(self, star):

        all_sources = set()
        predicates2tp = {}
        for triple_pattern in star.triple_patterns:
            predicate = triple_pattern[1]

            if predicate.value[0] == "<":
                predicate = predicate[1:-1]

            predicates2tp.setdefault(predicate, []).append(triple_pattern)

            relevant_sources = self.predicate2source.get(predicate, {})

            for relevant_source, cnt in relevant_sources.items():
                source_uri = self.dataset2uri[relevant_source]
                triple_pattern.sources[source_uri] = cnt
                all_sources.add(relevant_source)
                triple_pattern.cardinality = cnt

            #if len(relevant_sources) == 0:
            #    all_sources = self.dataset2uri.keys()
            #    for dataset, uri in self.dataset2uri.items():
                    # TODO: Update card estimation
                    # If we don't find a relevant source, we could assume that the estimated cardinality per source
                    # is given by the minimum cardinality of all predicates in that source
                    # Idea: we assume that if we didn't sample the predicate it must occur less frequently than the
                    # least frequent in our sample
            #        triple_pattern.sources[uri] = self.source2min_predicate[dataset]#1

        return predicates2tp, all_sources