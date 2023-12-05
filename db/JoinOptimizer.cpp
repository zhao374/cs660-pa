#include <db/JoinOptimizer.h>
#include <db/PlanCache.h>
#include <cfloat>

using namespace db;

/**
  * Estimate the cost of a join.
  *
  * The cost of the join should be a function of the amount of data that must
  * be read over the course of the query, as well as the number of CPU operations
  * performed by your join.
  * Assume that the cost of a single predicate application is roughly 1.
  *
  * @param card1
  *            Estimated cardinality of the left-hand side of the query
  * @param card2
  *            Estimated cardinality of the right-hand side of the query
  * @param cost1
  *            Estimated cost of one full scan of the table on the left-hand side of the query
  * @param cost2
  *            Estimated cost of one full scan of the table on the right-hand side of the query
  * @return An estimate of the cost of this query, in terms of cost1 and cost2
  */
double JoinOptimizer::estimateJoinCost(int card1, int card2, double cost1, double cost2) {
    return cost1 + (card1 * cost2) + (card1 * card2);
    // TODO pa4.2: some code goes here
}

/**
 * Estimate the join cardinality of two tables.
 */
int JoinOptimizer::estimateTableJoinCardinality(Predicate::Op joinOp,
                                                const std::string &table1Alias, const std::string &table2Alias,
                                                const std::string &field1PureName, const std::string &field2PureName,
                                                int card1, int card2, bool t1pkey, bool t2pkey,
                                                const std::unordered_map<std::string, TableStats> &stats,
                                                const std::unordered_map<std::string, int> &tableAliasToId) {
    std::string table1Name = Database::getCatalog().getTableName(tableAliasToId.at(table1Alias));
    std::string table2Name = Database::getCatalog().getTableName(tableAliasToId.at(table2Alias));
    int index1 = Database::getCatalog().getTupleDesc(tableAliasToId.at(table1Alias)).fieldNameToIndex(field1PureName);
    int index2 = Database::getCatalog().getTupleDesc(tableAliasToId.at(table2Alias)).fieldNameToIndex(field2PureName);
    TableStats tableStats1 = stats.at(table1Name);
    TableStats tableStats2 = stats.at(table2Name);

    int card = 1;
    switch (joinOp) {
        case Predicate::Op::EQUALS:
            if (t1pkey && !t2pkey){
                card=card2;
            } else if (t2pkey && !t1pkey){
                card=card1;
            } else {
                //R: if both primary key or neither primary key, return the bigger one
                card = card1>card2 ? card1 : card2;
            }
            break;

        default:
            card = (int) (0.3* card1 * card2);
            break;
    }

    return card <= 0 ? 1 : card;
    // TODO pa4.2: some code goes here
}

/**
 * Compute a logical, reasonably efficient join on the specified tables. See
 * PS4 for hints on how this should be implemented.
 *
 * @param stats
 *            Statistics for each table involved in the join, referenced by
 *            base table names, not alias
 * @param filterSelectivities
 *            Selectivities of the filter predicates on each table in the
 *            join, referenced by table alias (if no alias, the base table
 *            name)
 * @return A std::vector<LogicalJoinNode> that stores joins in the left-deep
 *         order in which they should be executed.
 * @throws ParsingException
 *             when stats or filter selectivities is missing a table in the
 *             join, or or when another internal error occurs
 */
std::vector<LogicalJoinNode> JoinOptimizer::orderJoins(std::unordered_map<std::string, TableStats> stats,
                                                       std::unordered_map<std::string, double> filterSelectivities) {
    // TODO: how can we represent the output of the first join, in order to create a LogicalJoinNode object for the second join?

    std::unordered_set<LogicalJoinNode> joinSet;
    for (const auto &join : joins) {
        joinSet.insert(join);
    }

    std::vector<std::unordered_set<LogicalJoinNode>> j = enumerateSubsets(joins, 1);
    PlanCache optjoin;

    int sizeOfj = j.size();
    for (int i = 1; i <= sizeOfj; i++) {
        std::vector<std::unordered_set<LogicalJoinNode>>  lenISubsetOfj = enumerateSubsets(joins, i);
        for (const auto &s : lenISubsetOfj) {
            double bestCostSoFar = std::numeric_limits<double>::max();
            int bestCardSofar = std::numeric_limits<int>::max();
            std::vector<LogicalJoinNode> bestPlanSofar;

            for (const auto &s1 : s) {
                CostCard * costcard = computeCostAndCardOfSubplan(stats, filterSelectivities, s1, s, std::numeric_limits<double>::max(), optjoin);

                if (costcard->cost < bestCostSoFar) {
                    optjoin.addPlan(const_cast<std::unordered_set<LogicalJoinNode> &>(s), costcard);
                    bestPlanSofar = costcard->plan;
                    bestCardSofar = costcard->card;
                    bestCostSoFar = costcard->cost;
                }
            }

        }
    }
    std::vector<LogicalJoinNode> bestJoinOrder = optjoin.get(joinSet)->plan;
    return bestJoinOrder;

}
