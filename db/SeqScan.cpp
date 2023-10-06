#include <db/SeqScan.h>
#include "db/HeapFile.h"

using namespace db;

// TODO pa1.6: implement
SeqScan::SeqScan(TransactionId *tid, int tableid, const std::string &tableAlias) {
    this->tid = *tid;
    this->tableId = tableid;
    this->tableAlias = tableAlias;
}

std::string SeqScan::getTableName() const {
    return Database::getCatalog().getTableName(tableId);
    // TODO pa1.6: implement
}

std::string SeqScan::getAlias() const {
    return tableAlias;
    // TODO pa1.6: implement
}

void SeqScan::reset(int tabid, const std::string &tableAlias) {
    this->tableId = tabid;
    // TODO pa1.6: implement
}

const TupleDesc &SeqScan::getTupleDesc() const {
    return Database::getCatalog().getTupleDesc(tableId);
    // TODO pa1.6: implement
}

SeqScan::iterator SeqScan::begin() const {
    db::HeapPage *myPage = dynamic_cast<HeapPage *>(db::Database::getCatalog().getDatabaseFile(tableId)->readPage(
            HeapPageId(tableId, 0)));
    return myPage->begin();
}

SeqScan::iterator SeqScan::end() const {
    db::HeapPage *myPage = dynamic_cast<HeapPage *>(db::Database::getCatalog().getDatabaseFile(tableId)->readPage(
            HeapPageId(tableId, 0)));
    // std::vector<Tuple> myTuples;
    return myPage->end();
    // TODO pa1.6: implement
}