#include <db/IntegerAggregator.h>
#include <db/IntField.h>
#include <unordered_map>
#include "db/Operator.h"

using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
    int gbfield;
    std::unordered_map<int, IntAggregateData> aggMap;
    std::unordered_map<int, IntAggregateData>::iterator aggMapIter;
    TupleDesc resultTD;
    Aggregator::Op what;
public:
    IntegerAggregatorIterator(int gbfield,
                              const std::unordered_map<int, IntAggregateData> &data)
                              :aggMap(data), gbfield(gbfield){
        this->resultTD = *new TupleDesc({Types::INT_TYPE});

    }

    void open() override {
        this->aggMapIter = this->aggMap.begin();
    }

    bool hasNext() override {
        return (this->aggMapIter != aggMap.end());
    }

    Tuple next() override {
        auto next = *this->aggMapIter;
        IntAggregateData& helper = next.second;
        int retField;
        switch (this->what) {
            case Aggregator::Op::COUNT:
                retField = helper.getCount();
                break;
            case Aggregator::Op::MIN:
                retField = helper.getMin();
                break;
            case Aggregator::Op::MAX:
                retField = helper.getMax();
                break;
            case Aggregator::Op::AVG:
                if (helper.getCount() == 0) {
                    retField = 0;
                } else {
                    retField = helper.getSum() / helper.getCount();
                }
                break;
            case Aggregator::Op::SUM:
                retField = helper.getSum();
                break;
            default:break;
        }
        Tuple tuple = Tuple(this->resultTD);
        if (this->gbfield == Aggregator::NO_GROUPING) {
            // no group by
            IntField myField = IntField(retField);
            tuple.setField(0, &myField);
        } else {
            IntField myField = IntField(next.first);
            tuple.setField(0, &myField);
            myField = IntField(retField);
            tuple.setField(1, &myField);
        }
        ++this->aggMapIter;
        return tuple;
    }

    void rewind() override {
        this->close();
        this->open();
    }

    const TupleDesc &getTupleDesc() const override {
        return this->resultTD;
    }

    void close() override {
        this->aggMap.clear();
        this->aggMapIter = aggMap.end();
    }
};

IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Aggregator::Op what) {
    this->gbfield = gbfield;
    this->gbfieldType = gbfieldtype;
    this->afield = afield;
    this->what = what;
    // TODO pa3.2: some code goes here
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    IntAggregateData aggData;
    IntField aggField = (IntField &&) tup->getField(this->afield);
    if (this->gbfield == NO_GROUPING) {
        this->intMap[0].data.push_back(aggField.getValue());
        aggData = this->intMap[0];
    } else {
        IntField field = (IntField &&) tup->getField(this->gbfield);

        if (this->gbfieldType == Types::INT_TYPE) {
            IntField* intField = &field;
            int intKey = intField->getValue();
            if (this->intMap.find(intKey) == this->intMap.end()) {
                this->intMap[intKey] = IntAggregateData();
            }
            aggData = this->intMap[intKey];
        }
        aggData.data.push_back(aggField.getValue());
    }

    aggData.setMin(std::min(aggData.getMin(), aggField.getValue()));
    aggData.setMax(std::max(aggData.getMax(), aggField.getValue()));
    aggData.setSum(aggData.getSum() + aggField.getValue());
    aggData.increaseCount();

    // TODO pa3.2: some code goes here
}

DbIterator *IntegerAggregator::iterator() const {
    return new IntegerAggregatorIterator(this->gbfield,this->intMap);
    // TODO pa3.2: some code goes here
}
