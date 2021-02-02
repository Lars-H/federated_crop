from itertools import product, combinations
from crop.query_plan_optimizer.physical_plan import PhysicalPlan
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.operators.xunion import Xunion
from nlde.operators.xnoptional import Xnoptional
from nlde.operators.xgoptional import Xgoptional
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion
from crop.source_selection.utils import StarSubquery
from crop.costmodel.cardinality_estimation import CardinalityEstimation
from crop.source_selection import CharSet_Selector
import math
from nlde.query import UnionBlock, JoinBlock, Optional, Filter, BGP, TriplePattern
from crop.query_plan_optimizer.optimizer import Optimizer


import logging
logger = logging.getLogger("nlde_logger")

PAGE_SIZE = 100

class Charset_Optimizer(Optimizer):

    def __init__(self, **kwargs):

        self.eddies = kwargs.get("eddies", 2)
        self.sources = kwargs.get("sources", [])
        self.poly = True

        self.merge_excl_grps = kwargs.get("merge_eg", False)

        self.source_selector = CharSet_Selector(**kwargs)
        self.cardinality_estimation = CardinalityEstimation()

        # TPF Config
        self.tpf_page_size = kwargs.get("tpf_page_size", 100)

        # brTPF Config
        self.brtpf_mapping_cnt = kwargs.get("brtpf_mappings", 30)

        # SPARQL Config
        self.sparql_limit = 10000
        self.sparql_mapping_cnt = kwargs.get("sparql_mappings", 50)

        self.number_of_subqueries = 0
        self.select_sources = []

    def __str__(self):
        params = self.params
        return "\t".join(params)

    @property
    def params(self):
        params = []
        return params

    @property
    def params_dct(self):
        params = {
            "optimizer" : "leftdeep",
            "poly" : str(self.poly),
        }
        params.update(self.source_selector.params)
        return params


    def union_subplans(self, subplans):
        if len(subplans) == 1:
            return subplans[0]
        else:
            return LogicalUnion(subplans, Xunion)

    def optimize_subquery(self, subqueries, filters):

        plans = []

        access_plans = []
        for subquery in subqueries:
            access_plans.append(LogicalPlan(subquery))

        todo = sorted(access_plans, key=lambda x: x.cardinality)
        plan = todo[0]
        todo.remove(plan)

        while len(todo):
            for i in range(len(todo)):
                if len(plan.variables.intersection(todo[i].variables)) > 0:
                    plan = LogicalPlan(plan, todo[i], self.get_physical_join_operator(plan, todo[i]))
                    plan.compute_cardinality(self.cardinality_estimation)
                    todo.remove(todo[i])
                    break
            else:
                # In case we cannot find another join able triple pattern
                next_tp = todo[0]
                plan = LogicalPlan(plan, next_tp, self.get_physical_join_operator(plan, next_tp))
                plan.compute_cardinality(self.cardinality_estimation)
                todo.remove(next_tp)

            plan.filters = filters
            plans.append(plan)
        if len(plans) == 0:
            return None
        plan = self.union_subplans(plans)
        return plan


    def decomposition_to_plan(self, decomposition):
        access_plans = []
        filters = []
        for subplan in decomposition:
            if isinstance(subplan, Filter):
                filters.append(subplan)
            else:
                if isinstance(subplan, BGP):
                    access_plans.append(LogicalPlan(subplan))
                else:
                    access_plans.append(LogicalPlan(subplan))

        todo = sorted(access_plans, key=lambda x: x.cardinality)
        plan = todo[0]
        todo.remove(plan)

        root = True

        while len(todo):
            for i in range(len(todo)):
                if len(plan.variables.intersection(todo[i].variables)) > 0:

                    join_operator = self.get_physical_join_operator(plan, todo[i])

                    if root and plan.is_basic_graph_pattern and join_operator == Xnjoin:
                        plan = LogicalUnion([plan])

                    plan = LogicalPlan(plan, todo[i], join_operator)
                    plan.compute_cardinality(self.cardinality_estimation)
                    todo.remove(todo[i])
                    root = False
                    break
            else:
                # In case we cannot find another join able triple pattern
                next_tp = todo[0]
                join_operator = self.get_physical_join_operator(plan, next_tp)
                plan = LogicalPlan(plan, next_tp, join_operator)
                plan.compute_cardinality(self.cardinality_estimation)
                todo.remove(next_tp)

        plan.filters = filters
        return plan


    def get_physical_join_operator(self, left_plan, right_plan):

        if isinstance(right_plan, LogicalUnion):
            nlj_sum = 0
            hj_sum = 0
            for union_subplan in right_plan.subplans:
                nlj_requests, hj_requests  = self.get_requests(union_subplan.left, left_plan.cardinality)
                nlj_sum += nlj_requests
                hj_sum += hj_requests
        else:
            nlj_requests, hj_requests  = self.get_requests(right_plan.left, left_plan.cardinality)

        logger.debug("NLJ requests: {}; HJ requests: {}".format(nlj_requests, hj_requests))

        if right_plan.is_triple_pattern and not right_plan.is_basic_graph_pattern:
            if right_plan.left.variable_position == 5 and right_plan.left[1].value == \
                                                       "<http://www.w3.org/2002/07/owl#sameAs>":
                return Xnjoin
            if right_plan.left.variable_position == 7:
                # Always use NLJ for <?s,?p,?o>
                return Xnjoin

        # Assumption: A NLJ Requests is considered cheaper than a HJ request
        if nlj_requests <= hj_requests:
            return Xnjoin
        else:
            return Fjoin

    def get_requests(self, plan, card_left):

        request_sum_nlj = 0
        request_sum_hj = 0
        for source, card in plan.sources.items():
            if source.startswith("sparql@"):
                if self.poly:
                    request_sum_nlj += math.ceil(float(card_left) / float(self.sparql_mapping_cnt))
                else:
                    request_sum_nlj += card_left
                request_sum_hj += math.ceil(float(card) / float(self.sparql_limit))

            elif source.startswith("brtpf@"):
                if self.poly:
                    request_sum_nlj += math.ceil(float(card_left) / float(self.brtpf_mapping_cnt))
                else:
                    request_sum_nlj += card_left
                request_sum_hj += math.ceil(float(card) /float(self.tpf_page_size))

            else:
                request_sum_nlj += math.ceil(float(card_left))
                request_sum_hj += math.ceil(float(card) /float(self.tpf_page_size))

        return request_sum_nlj, request_sum_hj

    def merge_subqueries(self, sq1, sq2):

        if isinstance(sq1, TriplePattern):
            if isinstance(sq2, TriplePattern):
                return BGP([sq1, sq2])
            else:
                return BGP([sq1] + sq2.triple_patterns)
        else:
            if isinstance(sq2, TriplePattern):
                return BGP(sq1.triple_patterns + [sq2])
            else:
                return BGP(sq1.triple_patterns + sq2.triple_patterns)

    def merge_exclusive_groups(self, subqueries):

        to_remove = set()
        to_add = []
        for sq1, sq2 in combinations(subqueries,2):
            if sq1.compatible(sq2):
                if len(sq1.sources) == 1 and len(sq2.sources) == 1:
                    source_intersection = set(sq1.sources.keys()).intersection(set(sq2.sources.keys()))
                    if len(source_intersection) == 1:
                        # Found an exlcuive group
                        to_remove.add(sq1)
                        to_remove.add(sq2)
                        new_bgp = self.merge_subqueries(sq1, sq2)
                        new_bgp.cardinality = min(sq1.cardinality, sq2.cardinality)
                        to_add.append(new_bgp)
                        break
        for remove in to_remove:
            subqueries.remove(remove)

        subqueries.extend(to_add)
        return subqueries


    def optimize_bgp(self, triple_patterns):
        ssqs, filters = self.starshaped_subqueries(triple_patterns)


        all_tps = []
        for join_var, ssq in ssqs.items():
            elems = self.source_selector.execute_source_selection(ssq)
            all_tps.extend(elems)

        # TODO: Merge Exclusive Groups as well
        # Note: We could merge Exclusive groups, however, we could not be sure that they are exclusive groups in case
        # we rely on sampled statistics. This is because, we do not know if we are potentially missing a source which
        # would make the set of tps a non-exclusive groups
        if self.merge_excl_grps:
            # Try to merge exclusive groups as long as possible
            while True:
                cnt = len(all_tps)
                tmp_tps = self.merge_exclusive_groups(all_tps)
                if len(tmp_tps) == cnt:
                    break


        # Number of subqueries
        self.number_of_subqueries += len(all_tps)

        # Select sources
        for tp in all_tps:
            self.select_sources.extend(tp.sources.keys())

        # Optimize Star Query
        plan = self.decomposition_to_plan(all_tps)
        plan.filters = filters
        return plan

    def create_plan(self, query):
        self.number_of_subqueries = 0
        self.query = query
        logical_plan = self.get_logical_plan(query.body)
        physical_plan = PhysicalPlan(self.sources, self.eddies, logical_plan, query, poly_operator=self.poly,
                                     sparql_limit=self.sparql_limit, sparql_mappings=self.sparql_mapping_cnt,
                                     brtpf_mappings=self.brtpf_mapping_cnt)
        return physical_plan

    def starshaped_subqueries(self, triple_patterns):

        ssqs = {}
        filters = []
        for tp in triple_patterns:
            if isinstance(tp, Filter):
                filters.append(tp)
                continue
            var = str(tp[0])
            if var not in ssqs.keys():
                ssq = StarSubquery([tp], var)
                ssqs[var] = ssq
            else:
                ssqs[var].triple_patterns.append(tp)
        return ssqs, filters


    def get_optional_operator(self, left_plan, right_plan):

        return Xnoptional
        #xn_requests = math.ceil(left_plan.cardinality / PAGE_SIZE) + left_plan.cardinality
        #xg_requests = math.ceil(left_plan.cardinality / PAGE_SIZE) + math.ceil(right_plan.cardinality / PAGE_SIZE)

        # Decide which optional Operator to place
        #if len(right_plan.triple_patterns) == 1 and xn_requests < xg_requests:
        #    return Xnoptional
        #else:
        #    return Xgoptional
        #return l_plan


    def groupby_sources(self, triple_patterns):

        source_groups = {}
        for triple_pattern in triple_patterns:
            for source in triple_pattern.sources.keys():
                source_groups.setdefault(source, []).append(triple_pattern)

        return source_groups