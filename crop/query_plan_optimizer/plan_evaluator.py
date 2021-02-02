from crop.query_plan_optimizer.logical_plan import LogicalUnion
from nlde.engine.contact_source import get_metadata

class Plan_Evaluator(object):


    def __init__(self, sources, plan):

        self.sources = sources
        self.plan = plan

        self.results = []
        self.q_errors = []

    def evaluate_plan(self):

        plan = self.plan
        self.evaluate_node(plan)
        #Muself.evaluate_node(plan.R)

    def evaluate_node(self, node):

        if node.is_triple_pattern:

            estimated_cardinality = node.L.cardinality
            estimation_case = node.L.__dict__.get("estimation_case", -1)
            count  = get_metadata(node.L.sources.keys(),node.L)
            #print(node.L, count,  estimated_cardinality)
            try:
                q_error = max(estimated_cardinality/count, count/estimated_cardinality)
                self.q_errors.append(q_error)
            except ZeroDivisionError:
                #print(estimated_cardinality, count)
                q_error = "NA"
            self.results.append({
                "estimated" : estimated_cardinality,
                    "true" : count,
                "q_error" : q_error,
                "subquery" : str(node.L),
                "estimation_case" : estimation_case
            })


        elif isinstance(node, LogicalUnion):
            for subnode in node .subplans:
                self.evaluate_node(subnode)

        else:
            self.evaluate_node(node.L)
            self.evaluate_node(node.R)