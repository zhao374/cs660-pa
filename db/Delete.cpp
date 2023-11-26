#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>
#include <db/Type.h>
using namespace db;

Delete::Delete(TransactionId t, DbIterator *child):t(t),child(child) {

    std::vector<Types::Type> typeT={Types::INT_TYPE};
    std::vector<std::string> fieldF={"DELETE_CNT"};
    this->cntTD = *(new TupleDesc(typeT,fieldF));
    // TODO pa3.3: some code goes here
}

const TupleDesc &Delete::getTupleDesc() const {
    return this->cntTD;
    // TODO pa3.3: some code goes here
}

void Delete::open() {
    this->child->open();
    this->used = false;
    Operator::open();
    // TODO pa3.3: some code goes here
}

void Delete::close() {
    this->child->close();
    Operator::close();
    // TODO pa3.3: some code goes here
}

void Delete::rewind() {
    this->child->rewind();
    this->used = false;
    // TODO pa3.3: some code goes here
}

std::vector<DbIterator *> Delete::getChildren() {
    return {this->child};
    // TODO pa3.3: some code goes here
}

void Delete::setChildren(std::vector<DbIterator *> children) {
    this->child=children[0];
    // TODO pa3.3: some code goes here
}

std::optional<Tuple> Delete::fetchNext() {
    int count = 0;
    while (this->child->hasNext()) {
        Tuple tt=this->child->next();
        db::Database::getBufferPool().deleteTuple(t, &tt);
        ++count;
    }
    if (!this->used) {
        Tuple tp = Tuple(this->cntTD);
        tp.setField(0, new IntField(count));
        this->used = true;
        return tp;
    } else {
        return {};
    }
    // TODO pa3.3: some code goes here
}
