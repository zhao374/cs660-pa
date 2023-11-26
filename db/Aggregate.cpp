#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {
    if (this->aggrIter != nullptr && this->aggrIter->hasNext()) {
        return this->aggrIter->next();
    }
}

Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop) {
    this->child = child;
    this->aField = afield;
    this->gField = gfield;

    TupleDesc tmpChildTD = this->child->getTupleDesc();
    this->aFieldType = tmpChildTD.getFieldType(this->aField);

    // appLabel is used in the precalc-ed merged Tuple Desc.
    std::string aggLabel = to_string(aop) + "(" +
                           tmpChildTD.getFieldName(this->aField) + ")";

    std::vector<Types::Type> tpType(2);
    std::vector<std::string> tpLabel(2);

    if (this->gField != Aggregator::NO_GROUPING) {
        this->gFieldType = tmpChildTD.getFieldType(this->gField);
        tpType[0] = this->gFieldType;
        tpType[1] = this->aFieldType;
        tpLabel[0] = tmpChildTD.getFieldName(this->gField);
        tpLabel[1] = aggLabel;
    } else {
        this->gFieldType = Types::INT_TYPE; // default value
        tpType[0] = this->aFieldType;
        tpLabel[0] = aggLabel;
    }

    this->op = aop;
    this->mergedTp = TupleDesc(tpType, tpLabel);
}

int Aggregate::groupField() {
    return this->gField;
}

std::string Aggregate::groupFieldName() {
    if (this->gField != Aggregator::NO_GROUPING) {
        return this->child->getTupleDesc().getFieldName(this->gField);
    } else {
        return nullptr;
    }
}

int Aggregate::aggregateField() {
    return this->aField;
}

std::string Aggregate::aggregateFieldName() {
    TupleDesc td = this->child->getTupleDesc();
    assert(this->aField >= 0);
    assert(this->aField < td.numFields());
    return td.getFieldName(this->aField);
}

Aggregator::Op Aggregate::aggregateOp() {
    return this->op;
}

void Aggregate::open() {
    this->child->open();
    Aggregator* agg;

    if (this->aFieldType == Types::INT_TYPE) {
        agg = new IntegerAggregator(this->gField, this->gFieldType, this->aField, this->op);
    } else {
        agg = new StringAggregator(this->gField, this->gFieldType, this->aField, this->op);
    }

    while (this->child->hasNext()) {
        Tuple temp = this->child->next();
        agg->mergeTupleIntoGroup(&temp);
    }

    this->aggrIter = agg->iterator();
    this->aggrIter->open();
}

void Aggregate::rewind() {
    if (this->aggrIter != nullptr) {
        this->aggrIter->rewind();
    } else {
        this->close();
        this->open();
    }
}

const TupleDesc &Aggregate::getTupleDesc() const {
    return this->mergedTp;
}

void Aggregate::close() {
    this->child->close();
    this->aggrIter = nullptr;
}

std::vector<DbIterator *> Aggregate::getChildren() {
    return {child};

}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
    child = children[0];
}
