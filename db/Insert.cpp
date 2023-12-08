#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;

std::optional<Tuple> Insert::fetchNext() {
    // TODO pa3.3: some code goes here
    if (done) return std::nullopt;
    done = true;
    int count = 0;
    BufferPool &bufferPool = Database::getBufferPool();
    while (child->hasNext()) {
        Tuple next = child->next();
        bufferPool.insertTuple(transactionId, tableId, &next);
        count++;
    }
    Tuple result(td);
    result.setField(0, new IntField(count));
    return result;
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId) {
    // TODO pa3.3: some code goes here
    transactionId = t;
    this->child = child;
    this->tableId = tableId;
    td = TupleDesc({Types::Type::INT_TYPE});
}

const TupleDesc &Insert::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return td;
}

void Insert::open() {
    // TODO pa3.3: some code goes here
    child->open();
    done = false;
    Operator::open();
}

void Insert::close() {
    // TODO pa3.3: some code goes here
    child->close();
    Operator::close();
}

void Insert::rewind() {
    // TODO pa3.3: some code goes here
    child->rewind();
    Operator::rewind();
}

std::vector<DbIterator *> Insert::getChildren() {
    // TODO pa3.3: some code goes here
    return {child};
}

void Insert::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    child = children[0];
}
