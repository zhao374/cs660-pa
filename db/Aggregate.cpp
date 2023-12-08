#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {
    // TODO pa3.2: some code goes here
    return aggregatorIt->hasNext() ? std::make_optional(aggregatorIt->next()) : std::nullopt;
}

Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop) {
    // TODO pa3.2: some code goes here
    it = child;
    this->afield = afield;
    this->gfield = gfield;
    std::optional<Types::Type> gField;
    gfield != Aggregator::NO_GROUPING ? std::make_optional(it->getTupleDesc().getFieldType(gfield)) : std::nullopt;
    this->aop = aop;
    switch (it->getTupleDesc().getFieldType(afield)) {
        case Types::INT_TYPE:
            aggregator = new IntegerAggregator(gfield, gField, afield, aop);
            break;
        case Types::STRING_TYPE:
            aggregator = new StringAggregator(gfield, gField, afield, aop);
            break;
    }
}

int Aggregate::groupField() {
    // TODO pa3.2: some code goes here
    return gfield;
}

std::string Aggregate::groupFieldName() {
    // TODO pa3.2: some code goes here
    return gfield != Aggregator::NO_GROUPING ? it->getTupleDesc().getFieldName(gfield) : "";
}

int Aggregate::aggregateField() {
    // TODO pa3.2: some code goes here
    return afield;
}

std::string Aggregate::aggregateFieldName() {
    // TODO pa3.2: some code goes here
    return it->getTupleDesc().getFieldName(afield);
}

Aggregator::Op Aggregate::aggregateOp() {
    // TODO pa3.2: some code goes here
    return aop;
}

void Aggregate::open() {
    // TODO pa3.2: some code goes here
    Operator::open();
    it->open();
    while (it->hasNext()) {
        Tuple next = it->next();
        aggregator->mergeTupleIntoGroup(&next);
    }
    it->close();
    aggregatorIt = aggregator->iterator();
    aggregatorIt->open();
}

void Aggregate::rewind() {
    // TODO pa3.2: some code goes here
    Operator::rewind();
    aggregatorIt->rewind();
}

const TupleDesc &Aggregate::getTupleDesc() const {
    // TODO pa3.2: some code goes here
    return aggregatorIt->getTupleDesc();
}

void Aggregate::close() {
    // TODO pa3.2: some code goes here
    aggregatorIt->close();
    Operator::close();
}

std::vector<DbIterator *> Aggregate::getChildren() {
    // TODO pa3.2: some code goes here
    return {it};
}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.2: some code goes here
    it = children[0];
}
