#include <db/SeqScan.h>

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

    std::vector<Tuple> myTuples;
    return SeqScanIterator{0, myTuples.size(), myTuples};
}

SeqScan::iterator SeqScan::end() const {
    std::vector<Tuple> myTuples;
    return SeqScanIterator{0, myTuples.size(), myTuples};
    // TODO pa1.6: implement
}