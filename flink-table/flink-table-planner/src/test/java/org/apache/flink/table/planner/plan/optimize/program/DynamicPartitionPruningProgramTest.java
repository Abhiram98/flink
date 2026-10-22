/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.optimize.program;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.col;

/**
 * Tests for rules that extend {@link FlinkDynamicPartitionPruningProgram} to create {@link
 * org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan}.
 */
class DynamicPartitionPruningProgramTest extends TableTestBase {
    private final BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());
    private final TestValuesCatalog catalog =
            new TestValuesCatalog("testCatalog", "test_database", true);

    @BeforeEach
    void setup() {
        catalog.open();
        util.tableEnv().registerCatalog("testCatalog", catalog);
        util.tableEnv().useCatalog("testCatalog");
        TableConfig tableConfig = util.tableEnv().getConfig();
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED, true);

        // partition fact table.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE fact_part (\n"
                                + "  identifier BIGINT,\n"
                                + "  name STRING,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  fact_date_sk BIGINT\n"
                                + ") PARTITIONED BY (fact_date_sk)\n"
                                + "WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'runtime-source' = 'NewSource',\n"
                                + " 'partition-list' = 'fact_date_sk:1990;fact_date_sk:1991;fact_date_sk:1992',\n"
                                + " 'dynamic-filtering-fields' = 'fact_date_sk;amount',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        // dim table.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE dim (\n"
                                + "  identifier BIGINT,\n"
                                + "  male BOOLEAN,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  dim_date_sk BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'runtime-source' = 'NewSource',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
    }

    @Test
    void testLargeQueryPlanShouldNotOutOfMemoryWithTableApi() {
        // TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED is already enabled
        List<String> selectStmts = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            util.tableEnv()
                    .executeSql(
                            "CREATE TABLE IF NOT EXISTS table"
                                    + i
                                    + "(att STRING,filename STRING) "
                                    + "with("
                                    + "     'connector' = 'values', "
                                    + "     'runtime-source' = 'NewSource', "
                                    + "     'bounded' = 'true'"
                                    + ")");
            selectStmts.add("select att,filename from table" + i);
        }

        final String countName = "CNM";
        Table allUnionTable = util.tableEnv().sqlQuery(String.join(" UNION ALL ", selectStmts));
        Table res =
                allUnionTable.join(
                        allUnionTable
                                .groupBy(col("att"))
                                .select(col("att"), col("att").count().as(countName))
                                .filter(col(countName).isGreater(1))
                                .select(col("att").as("l_key")),
                        col("att").isEqual(col("l_key")));
        util.verifyExecPlan(res);

        // clear resources
        for (int i = 0; i < 100; i++) {
            util.tableEnv().executeSql("DROP TABLE IF EXISTS table" + i);
        }
    }

    @Test
    void testLargeQueryPlanShouldNotOutOfMemoryWithSqlApi() {
        // TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED is already enabled
        List<String> selectStmts = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            util.tableEnv()
                    .executeSql(
                            "CREATE TABLE IF NOT EXISTS table"
                                    + i
                                    + "(att STRING,filename STRING) "
                                    + "with("
                                    + "     'connector' = 'values', "
                                    + "     'runtime-source' = 'NewSource', "
                                    + "     'bounded' = 'true'"
                                    + ")");
            selectStmts.add("select att,filename from table" + i);
        }

        final String countName = "CNM";
        final String unionSelectStmts = String.join(" UNION ALL ", selectStmts);

        final String groupedUnionStmt =
                String.format(
                        "SELECT att as l_key, COUNT(att) AS %s "
                                + "FROM (%s) "
                                + "GROUP BY att "
                                + "HAVING COUNT(att) > 1 ",
                        countName, unionSelectStmts);

        final String joinedUnionStmt =
                String.format(
                        "SELECT * FROM (%s) as t1 INNER JOIN (%s) as t2 ON t1.att = t2.l_key",
                        unionSelectStmts, groupedUnionStmt);
        Table resultTable = util.tableEnv().sqlQuery(joinedUnionStmt);

        util.verifyExecPlan(resultTable);

        // clear resources
        for (int i = 0; i < 100; i++) {
            util.tableEnv().executeSql("DROP TABLE IF EXISTS table" + i);
        }
    }

    @Test
    void testDimTableFilteringFieldsNotInJoinKey() {
        // fact_part.identifier not in dynamic-filtering-fields, so dynamic partition pruning will not
        // succeed.
        String query =
                "Select * from dim, fact_part where fact_part.identifier = dim.identifier and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDimTableWithoutFilter() {
        // If dim side without filters, dynamic partition pruning will not succeed.
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 100";
        util.verifyRelPlan(query);
    }

    @Test
    void testDimTableWithUnsuitableFilter() {
        // For filters in dim table side, they need to filter enough partitions. Like NOT NULL will
        // not succeed for dynamic partition pruning.
        String query =
                "Select * from dim join fact_part on fact_part.fact_date_sk = dim.dim_date_sk where dim.identifier is not null";
        util.verifyRelPlan(query);
    }

    @Test
    void testFactTableIsNotPartitionTable() {
        // non-partition fact table. Dynamic partition pruning will not succeed if fact side is not
        // partition table.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE none_part_fact (\n"
                                + "  identifier BIGINT,\n"
                                + "  name STRING,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  fact_date_sk BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'runtime-source' = 'NewSource',\n"
                                + " 'dynamic-filtering-fields' = 'fact_date_sk;amount',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        String query =
                "Select * from dim, none_part_fact where none_part_fact.fact_date_sk = dim.dim_date_sk"
                        + " and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testFactTableIsLegacySource() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE legacy_source (\n"
                                + "  identifier BIGINT,\n"
                                + "  name STRING,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  fact_date_sk BIGINT\n"
                                + ") PARTITIONED BY (fact_date_sk)\n"
                                + "WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'runtime-source' = 'SourceFunction',\n"
                                + " 'partition-list' = 'fact_date_sk:1990;fact_date_sk:1991;fact_date_sk:1992',\n"
                                + " 'dynamic-filtering-fields' = 'fact_date_sk;amount',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        String query =
                "Select * from dim, legacy_source where legacy_source.fact_date_sk = dim.dim_date_sk"
                        + " and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDimTableWithFilterPushDown() {
        // Even though have filter push down, dynamic partition pruning will succeed.
        String query =
                "Select * from fact_part join (Select * from dim) t1"
                        + " on fact_part.fact_date_sk = dim_date_sk where t1.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testJoinKeyIsDynamicFilterFieldNotPartitionKey() {
        // Not only partition key, but also dynamic filtering field in join key will succeed in
        // dynamic partition pruning.
        String query =
                "Select * from dim, fact_part where fact_part.amount = dim.amount and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDynamicFilteringFactInRightRule() throws TableNotExistException {
        // Base rule.
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDynamicFilteringFactInLeftRule() throws TableNotExistException {
        // Base rule.
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        String query =
                "Select * from fact_part, dim where fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDynamicFilteringFactInRightWithExchangeRule() {
        // Base rule.
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDynamicFilteringFactInLeftWithExchangeRule() {
        // Base rule.
        String query =
                "Select * from fact_part, dim where fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDynamicFilteringFactInRightWithCalcRule() throws TableNotExistException {
        // Base rule.
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDynamicFilteringFactInLeftWithCalcRule() throws TableNotExistException {
        // Base rule.
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        String query =
                "Select * from fact_part, dim where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDynamicFilteringFactInRightWithExchangeAndCalcRule() {
        // Base rule.
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDynamicFilteringFactInLeftWithExchangeAndCalcRule() {
        // Base rule.
        String query =
                "Select * from fact_part, dim where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testComplexCalcInFactSide() {
        // Although the partition key is converted, Dynamic Partition pruning can be successfully
        // applied.
        String query =
                "Select * from dim join (select fact_date_sk as fact_date_sk1, price + 1 as price1 from fact_part) t1"
                        + " on t1.fact_date_sk1 = dim_date_sk and t1.price1 > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testPartitionKeysIsComputeColumnsInFactSide() {
        // Dynamic filtering will not succeed for this query.
        String query =
                "Select * from dim join (select fact_date_sk + 1 as fact_date_sk1, price + 1 as price1 from fact_part) t1"
                        + " on t1.fact_date_sk1 = dim_date_sk and t1.price1 > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testPartitionKeysOrderIsChangedInFactSide() {
        // Dynamic filtering will succeed for this query.
        String query =
                "Select * from dim join (select fact_date_sk, identifier, name, amount, price from fact_part) t1"
                        + " on t1.fact_date_sk = dim_date_sk and t1.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testPartitionKeysNameIsChangedInFactSide() {
        // Dynamic filtering will succeed for this query.
        String query =
                "Select * from dim join (select identifier, name, amount, price, fact_date_sk as fact_date_sk1 from fact_part) t1"
                        + " on t1.fact_date_sk1 = dim_date_sk and t1.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testDynamicFilteringFieldIsComputeColumnsInFactSide() throws TableNotExistException {
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        // in this case. amount + 1 as amount is not a partition key, will succeed.
        String query =
                "Select * from dim join (select fact_date_sk, amount + 1 as amount from fact_part) t1 on"
                        + " fact_date_sk = dim_date_sk and t1.amount = dim.amount where dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testLeftOuterJoinWithFactInLeft() {
        // left outer join with fact in left will not succeed. Because if fact in left, filtering
        // condition is useless.
        String query =
                "Select * from fact_part left outer join dim on fact_part.fact_date_sk = dim.dim_date_sk"
                        + " where dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testLeftOutJoinWithFactInRight() {
        // left outer join with fact in right will succeed.
        String query =
                "Select * from dim left outer join fact_part on fact_part.fact_date_sk = dim.dim_date_sk"
                        + " where dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    void testSemiJoin() {
        // Now dynamic partition pruning support semi join, this query will succeed.
        String query =
                "Select * from fact_part where fact_part.fact_date_sk in"
                        + " (select dim_date_sk from dim where dim.price < 500)";
        util.verifyRelPlan(query);
    }

    @Test
    void testFullOuterJoin() {
        // Now dynamic partition pruning don't support full outer join.
        String query =
                "Select * from fact_part full outer join"
                        + " (select *  from dim where dim.price < 500) on fact_date_sk = dim_date_sk";
        util.verifyRelPlan(query);
    }

    @Test
    void testAntiJoin() {
        // Now dynamic partition prune don't support anti join.
        String query =
                "Select * from fact_part where not exists"
                        + " (select dim_date_sk from dim where dim.price < 500)";
        util.verifyRelPlan(query);
    }

    @Test
    void testMultiJoin() {
        // Another table.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sales (\n"
                                + "  identifier BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        String query =
                "Select * from fact_part, dim, sales where fact_part.identifier = sales.identifier and"
                        + " fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500 and dim.amount > 100";
        util.verifyRelPlan(query);
    }

    @Test
    void testComplexDimSideWithJoinInDimSide() {
        // TODO, Dpp will not success with complex dim side.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sales (\n"
                                + "  identifier BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE item (\n"
                                + "  identifier BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        String query =
                "Select * from fact_part join"
                        + " (select * from dim, sales, item where"
                        + " dim.identifier = sales.identifier and sales.identifier = item.identifier and dim.price < 500 and sales.price > 300) dimSide"
                        + " on fact_part.fact_date_sk = dimSide.dim_date_sk";
        util.verifyRelPlan(query);
    }

    @Test
    void testComplexDimSideWithAggInDimSide() {
        // Dim side contains agg will not succeed in this version, it will improve later.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sales (\n"
                                + "  identifier BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        String query =
                "Select * from fact_part join"
                        + " (select dim_date_sk, sum(dim.price) from dim where"
                        + "  dim.price < 500 group by dim_date_sk) dimSide"
                        + " on fact_part.fact_date_sk = dimSide.dim_date_sk";
        util.verifyRelPlan(query);
    }

    @Test
    void testDppWithoutJoinReorder() {
        // Dpp will success
        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);
        TableConfig tableConfig = util.tableEnv().getConfig();
        // Join reorder don't open.
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, false);

        String query =
                "Select * from fact_part, item, dim"
                        + " where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.identifier = item.identifier"
                        + " and dim.identifier = item.identifier "
                        + " and dim.price < 500 and dim.price > 300";
        util.verifyRelPlan(query);
    }

    @Test
    void testDppWithSubQuery() {
        // Dpp will success
        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);
        TableConfig tableConfig = util.tableEnv().getConfig();
        // Join reorder don't open.
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, false);

        String query =
                "Select * from fact_part, item, dim"
                        + " where fact_part.identifier = item.identifier"
                        + " and dim.price in (select price from dim where amount = (select amount from dim where amount = 2000))"
                        + " and fact_part.fact_date_sk = dim.dim_date_sk";
        util.verifyRelPlan(query);
    }

    @Test
    void testDppWithUnionInFactSide() {
        // Dpp will success.
        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from (select identifier, fact_date_sk, amount + 1 as amount1 from fact_part where price = 1 union all "
                        + "select identifier, fact_date_sk, amount + 1 from fact_part where price = 2) fact_part2, item, dim"
                        + " where fact_part2.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part2.identifier = item.identifier"
                        + " and dim.price < 500 and dim.price > 300";
        util.verifyRelPlan(query);
    }

    @Test
    void testDppWithAggInFactSideAndJoinKeyInGrouping() {
        // Dpp will success
        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from (Select fact_date_sk, item.amount, sum(fact_part.price) from fact_part "
                        + "join item on fact_part.identifier = item.identifier group by fact_date_sk, item.amount) t1 "
                        + "join dim on t1.fact_date_sk = dim.dim_date_sk where dim.price < 500 and dim.price > 300 ";
        util.verifyRelPlan(query);
    }

    @Test
    void testDppWithAggInFactSideAndJoinKeyInGroupFunction() {
        // Dpp will not success because join key in group function.
        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from (Select fact_part.identifier, item.amount, fact_part.name, sum(fact_part.price), sum(item.price), sum(fact_date_sk) as fact_date_sk1 "
                        + "from fact_part join item on fact_part.identifier = item.identifier "
                        + "group by fact_part.identifier, fact_part.name, item.amount) t1 "
                        + "join dim on t1.fact_date_sk1 = dim.dim_date_sk where dim.price < 500 and dim.price > 300 ";
        util.verifyRelPlan(query);
    }

    @Test
    void testDppWithAggInFactSideWithAggPushDownEnable() {
        // Dpp will not success while fact side source support agg push down and source agg push
        // down enabled is true.
        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from (Select identifier, amount, fact_date_sk, count(name), sum(price) "
                        + "from fact_part where fact_date_sk > 100 group by identifier, amount, fact_date_sk) t1 "
                        + "join dim on t1.fact_date_sk = dim.dim_date_sk where dim.price < 500 and dim.price > 300 ";
        util.verifyRelPlan(query);
    }

    @Test
    void testDppWithAggInFactSideWithAggPushDownDisable() {
        // Dpp will success while fact side source support agg push down but source agg push down
        // enabled is false.
        TableConfig tableConfig = util.tableEnv().getConfig();
        // Disable source agg push down.
        tableConfig.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED, false);

        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from (Select identifier, amount, fact_date_sk, count(name), sum(price) "
                        + "from fact_part where fact_date_sk > 100 group by identifier, amount, fact_date_sk) t1 "
                        + "join dim on t1.fact_date_sk = dim.dim_date_sk where dim.price < 500 and dim.price > 300 ";
        util.verifyRelPlan(query);
    }

    @Test
    void testDPPWithFactSideJoinKeyChanged() {
        // If partition keys changed in fact side. DPP factor will not success.
        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from (select fact_date_sk + 1 as fact_date_sk, identifier from fact_part) fact_part1 join item on "
                        + "fact_part1.identifier = item.identifier"
                        + " join dim on fact_part1.fact_date_sk = dim.dim_date_sk"
                        + " where dim.price < 500 and dim.price > 300";
        util.verifyRelPlan(query);
    }

    @Test
    void testDPPWithDimSideJoinKeyChanged() {
        // Although partition keys changed in dim side. DPP will success.
        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from fact_part join item on fact_part.identifier = item.identifier"
                        + " join (select dim_date_sk + 1 as dim_date_sk, price from dim) dim1"
                        + " on fact_part.fact_date_sk = dim1.dim_date_sk"
                        + " where dim1.price < 500 and dim1.price > 300";
        util.verifyRelPlan(query);
    }

    @Test
    void testDPPWithJoinKeysNotIncludePartitionKeys() {
        // If join keys of partition table join with dim table not include partition keys, dpp will
        // not success.
        String ddl =
                "CREATE TABLE test_database.item (\n"
                        + "  identifier BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from fact_part, item, dim"
                        + " where fact_part.identifier = dim.identifier"
                        + " and fact_part.identifier = item.identifier"
                        + " and dim.identifier = item.identifier "
                        + " and dim.price < 500 and dim.price > 300";
        util.verifyRelPlan(query);
    }

    @Test
    void testDppFactSideCannotReuseWithSameCommonSource() {
        String query =
                "SELECT * FROM(\n"
                        + " Select fact_part.identifier, fact_part.price, fact_part.amount from fact_part join (Select * from dim) t1"
                        + " on fact_part.fact_date_sk = dim_date_sk where t1.price < 500\n"
                        + " UNION ALL Select fact_part.identifier, fact_part.price, fact_part.amount from fact_part)";
        util.verifyExecPlan(query);
    }

    @Test
    void testDimSideReuseAfterProjectionPushdown() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE fact_part2 (\n"
                                + "  identifier BIGINT,\n"
                                + "  name STRING,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  fact_date_sk BIGINT\n"
                                + ") PARTITIONED BY (fact_date_sk)\n"
                                + "WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'runtime-source' = 'NewSource',\n"
                                + " 'partition-list' = 'fact_date_sk:1990;fact_date_sk:1991;fact_date_sk:1992',\n"
                                + " 'dynamic-filtering-fields' = 'fact_date_sk;amount',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        String query =
                "SELECT /*+ BROADCAST(dim) */ fact3.* FROM\n"
                        + "(SELECT /*+ BROADCAST(dim) */ fact.identifier, fact.price, fact.amount FROM (\n"
                        + " SELECT identifier, price, amount, fact_date_sk FROM fact_part "
                        + " UNION ALL SELECT identifier, price, amount, fact_date_sk FROM fact_part2) fact, dim\n"
                        + " WHERE fact_date_sk = dim_date_sk"
                        + " and dim.price < 500 and dim.price > 300)\n fact3 JOIN dim"
                        + " ON fact3.amount = dim.identifier AND dim.amount < 10";
        util.verifyExecPlan(query);
    }
}
