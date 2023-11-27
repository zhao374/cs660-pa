
#include <db/IntField.h>
#include <unordered_map>
#include "db/Operator.h"
#include "db/IntegerAggregator.h"

using namespace db;
IntAggregateData::IntAggregateData() {
    sum = 0;
    min = INT_MAX;
    max = INT_MIN;
    count = 0;
}

std::vector<int> IntAggregateData::getData() {
    return this->data;
}

int IntAggregateData::getMin() {return min;}

int IntAggregateData::getCount() {return count;};

int IntAggregateData::getMax() {return max;}

int IntAggregateData::getSum() {return sum;}

void IntAggregateData::setSum(int value) {sum = value;}

void IntAggregateData::setMax(int value) {max = value;}

void IntAggregateData::setMin(int value) {min = value;}

void IntAggregateData::increaseCount() {count++;}
class IntegerAggregatorIterator : public DbIterator {
private:
    int gbfield;
    std::unordered_map<int, IntAggregateData> aggMap;
    std::unordered_map<int, IntAggregateData>::iterator aggMapIter;
    TupleDesc resultTD;
    Aggregator::Op what;
public:
    IntegerAggregatorIterator(int gbfield,
                              const std::unordered_map<int, IntAggregateData> &data,
                              Aggregator::Op what)
                              :aggMap(data), gbfield(gbfield), what(what){
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

    IntField aggField = (IntField &&) tup->getField(this->afield);
    if (this->gbfield == NO_GROUPING) {
        this->intMap[0].data.push_back(aggField.getValue());
        this->intMap[0].setMin(std::min(this->intMap[0].getMin(), aggField.getValue()));
        this->intMap[0].setMax(std::max(this->intMap[0].getMax(), aggField.getValue()));
        this->intMap[0].setSum(this->intMap[0].getSum() + aggField.getValue());
        this->intMap[0].increaseCount();
    } else {
        IntField field = (IntField &&) tup->getField(this->gbfield);

        IntField* intField = &field;
        int intKey = intField->getValue();
        if (this->intMap.find(intKey) == this->intMap.end()) {
            this->intMap[intKey] = IntAggregateData();
        }

        this->intMap[intKey].data.push_back(aggField.getValue());
        this->intMap[intKey].setMin(std::min(this->intMap[intKey].getMin(), aggField.getValue()));
        this->intMap[intKey].setMax(std::max(this->intMap[intKey].getMax(), aggField.getValue()));
        this->intMap[intKey].setSum(this->intMap[intKey].getSum() + aggField.getValue());
        this->intMap[intKey].increaseCount();
    }



    // TODO pa3.2: some code goes here
}

DbIterator *IntegerAggregator::iterator() const {
    return new IntegerAggregatorIterator(this->gbfield,this->intMap, this->what);
    // TODO pa3.2: some code goes here
}
