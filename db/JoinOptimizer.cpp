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
    // TODO pa4.3: some code goes here
}
