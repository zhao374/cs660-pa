#include <db/Filter.h>

using namespace db;

Filter::Filter(Predicate p, DbIterator *child):p(p) {
    this->child=child;
    // TODO pa3.1: some code goes here
}

Predicate *Filter::getPredicate() {
    return &p;
    // TODO pa3.1: some code goes here
}

const TupleDesc &Filter::getTupleDesc() const {
    return this->child->getTupleDesc();
    // TODO pa3.1: some code goes here
}

void Filter::open() {
    this->child->rewind();
    Operator::open();
    // TODO pa3.1: some code goes here
}

void Filter::close() {
    Operator::close();
    // TODO pa3.1: some code goes here
}

void Filter::rewind() {
    this->open();
    // TODO pa3.1: some code goes here
}

std::vector<DbIterator *> Filter::getChildren() {
    return children;
    // TODO pa3.1: some code goes here
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    this->children=children;
    // TODO pa3.1: some code goes here
}

std::optional<Tuple> Filter::fetchNext() {
    Tuple t;
    while(this->child->hasNext()){
        t=this->child->next();
        if(this->p.filter(t)){
            return t;
        }
    }
    // TODO pa3.1: some code goes here
    return {};
}
