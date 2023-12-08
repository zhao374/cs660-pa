#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>

using namespace db;

Delete::Delete(TransactionId t, DbIterator *child) {
    // TODO pa3.3: some code goes here
    tid = t;
    this->child = child;
    td = TupleDesc({Types::Type::INT_TYPE});
    done = true;
}

const TupleDesc &Delete::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return td;
}

void Delete::open() {
    // TODO pa3.3: some code goes here
    Operator::open();
    done = false;
    child->open();
}

void Delete::close() {
    // TODO pa3.3: some code goes here
    child->close();
    Operator::close();
}

void Delete::rewind() {
    // TODO pa3.3: some code goes here
    Operator::rewind();
    child->rewind();
}

std::vector<DbIterator *> Delete::getChildren() {
    // TODO pa3.3: some code goes here
    return {child};
}

void Delete::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    child = children[0];
}

std::optional<Tuple> Delete::fetchNext() {
    // TODO pa3.3: some code goes here
    if (done) return std::nullopt;
    done = true;
    int count = 0;
    BufferPool &bufferPool = Database::getBufferPool();
    while (child->hasNext()) {
        Tuple next = child->next();
        bufferPool.deleteTuple(tid, &next);
        count++;
    }
    Tuple result(td);
    result.setField(0, new IntField(count));
    return result;
}
