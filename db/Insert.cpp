#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;

std::optional<Tuple> Insert::fetchNext() {
    int count = 0;
    while (this->child->hasNext()) {
            Tuple tt=this->child->next();
            db::Database::getBufferPool().insertTuple(this->tid, this->tableId, &tt);
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
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId) {
    this->tid = t;
    this->child = child;
    this->tableId = tableId;

    std::vector<Types::Type> typeT={Types::INT_TYPE};
    std::vector<std::string> fieldF={"INSERT_CNT"};
    this->cntTD = *(new TupleDesc(typeT, fieldF));

}

const TupleDesc &Insert::getTupleDesc() const {
    return this->cntTD;
}

void Insert::open() {
    this->child->open();
    this->used = false;
    Operator::open();
}

void Insert::close() {
    this->child->close();
    Operator::close();
}

void Insert::rewind() {
    this->child->rewind();
    this->used = false;
    // TODO pa3.3: some code goes here
}

std::vector<DbIterator *> Insert::getChildren() {
    return {this->child};
}

void Insert::setChildren(std::vector<DbIterator *> children) {
    this->child=children[0];
}
