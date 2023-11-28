#include <db/Join.h>

using namespace db;

Join::Join(JoinPredicate *p, DbIterator *child1, DbIterator *child2) {
    // TODO pa3.1: some code goes here
    predicate = p;
    outer = child1;
    inner = child2;
    td = TupleDesc::merge(outer->getTupleDesc(), inner->getTupleDesc());
}

JoinPredicate *Join::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return predicate;
}

std::string Join::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    return outer->getTupleDesc().getFieldName(predicate->getField1());
}

std::string Join::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    return inner->getTupleDesc().getFieldName(predicate->getField2());
}

const TupleDesc &Join::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return td;
}

void Join::open() {
    // TODO pa3.1: some code goes here
    outer->open();
    inner->open();
    Operator::open();
}

void Join::close() {
    // TODO pa3.1: some code goes here
    outer->close();
    inner->close();
    Operator::close();
}

void Join::rewind() {
    // TODO pa3.1: some code goes here
    outer->rewind();
    inner->rewind();
}

std::vector<DbIterator *> Join::getChildren() {
    // TODO pa3.1: some code goes here
    return {outer, inner};
}

void Join::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    outer = children[0];
    inner = children[1];
    td = TupleDesc::merge(outer->getTupleDesc(), inner->getTupleDesc());
}

std::optional<Tuple> Join::fetchNext() {
    // TODO pa3.1: some code goes here
    while (outerTuple.has_value() || outer->hasNext()) {
        if (!outerTuple.has_value()) {
            outerTuple = outer->next();
        }
        while (inner->hasNext()) {
            Tuple innerTuple = inner->next();
            if (!predicate->filter(&outerTuple.value(), &innerTuple)) {
                continue;
            }
            Tuple tuple(getTupleDesc());
            int i = 0;
            auto it1 = outerTuple->begin();
            while (it1 != outerTuple->end()) {
                tuple.setField(i, *it1);
                ++i;
                ++it1;
            }
            auto it2 = innerTuple.begin();
            while (it2 != innerTuple.end()) {
                tuple.setField(i, *it2);
                ++i;
                ++it2;
            }
            return tuple;
        }
        outerTuple = std::nullopt;
        inner->rewind();
    }
    return std::nullopt;
}
