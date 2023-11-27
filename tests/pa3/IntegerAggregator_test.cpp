#include <gtest/gtest.h>
#include <db/Database.h>
#include <db/HeapFile.h>
#include <db/SeqScan.h>
#include <db/IntField.h>
#include <db/IntegerAggregator.h>
#include <db/Utility.h>

TEST(IntegerAggregatorTest, test) {
    db::TupleDesc td = db::Utility::getTupleDesc(3);
    db::HeapFile table("table.dat", td);
    db::Database::getCatalog().addTable(&table, "t1");

    db::SeqScan ss1(table.getId(), "s1");
    db::IntegerAggregator agg(db::Aggregator::NO_GROUPING, std::nullopt, 1, db::Aggregator::Op::SUM);

    printf("ha\n");

    ss1.open();
    while (ss1.hasNext()) {
        auto tup = ss1.next();
         std::cout << tup.to_string() << std::endl;
        agg.mergeTupleIntoGroup(&tup);
    }
    printf("haha\n");

    ss1.close();
    auto agg_it = agg.iterator();
    printf("hahaha\n");

    int i = 0;
    agg_it->open();
    printf("hahahaha\n");
    while (agg_it->hasNext()) {
        db::Tuple tup=(agg_it->next());
        ++i;
        //std::cout << tup.to_string() << std::endl;
        //std::cout << tup.getField(0).to_string() << std::endl;
        int count = ((db::IntField &) tup.getField(0)).getValue();
        std::cout << tup.to_string() << std::endl;
        EXPECT_EQ(count, 12075);
    }
    printf("hahahahaha\n");
    agg_it->close();

    EXPECT_EQ(i, 1);
}
