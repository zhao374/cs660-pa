#include <db/Filter.h>

using namespace db;

Filter::Filter(Predicate p, DbIterator *child) : predicate(p), it(child) {
    // TODO pa3.1: some code goes here
}

Predicate *Filter::getPredicate() {
    // TODO pa3.1: some code goes here
    return &predicate;
}

const TupleDesc &Filter::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return it->getTupleDesc();
}

void Filter::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    it->open();
}

void Filter::close() {
    // TODO pa3.1: some code goes here
    it->close();
    Operator::close();
}

void Filter::rewind() {
    // TODO pa3.1: some code goes here
    it->rewind();
}

std::vector<DbIterator *> Filter::getChildren() {
    // TODO pa3.1: some code goes here
    return {it};
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    it = children[0];
}

std::optional<Tuple> Filter::fetchNext() {
    // TODO pa3.1: some code goes here
    while (it->hasNext()) {
        auto tuple = it->next();
        if (predicate.filter(tuple)) {
            return tuple;
        }
    }
    return std::nullopt;
}
