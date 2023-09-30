#include <db/HeapFile.h>
#include <db/TupleDesc.h>
#include <db/Page.h>
#include <db/PageId.h>
#include <db/HeapPage.h>
#include <stdexcept>
#include <sys/stat.h>
#include <fcntl.h>

using namespace db;

//
// HeapFile
//

#include <iostream>
#include <sstream>
#include <iomanip>

unsigned int hashFunction(const char* str) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }

    return static_cast<int>(hash);
}


// TODO pa1.5: implement
HeapFile::HeapFile(const char *fname, const TupleDesc &td) {
    this->td = td;

    this->tableId = hashFunction(fname);
    this->fname = fname;
}

int HeapFile::getId() const {
    return Database::getCatalog().getTableId(this->fname);
    // TODO pa1.5: implement
}

const TupleDesc &HeapFile::getTupleDesc() const {
    return td;
    // TODO pa1.5: implement
}

Page *HeapFile::readPage(const PageId &pid) {
    return Database::getBufferPool().getPage(TransactionId(), &pid);
    // TODO pa1.5: implement
}

int HeapFile::getNumPages() {
    return pages.size();
    // TODO pa1.5: implement
}

HeapFileIterator HeapFile::begin() const {
    return HeapFileIterator{0, pages.size(), pages};
    // TODO pa1.5: implement
}

HeapFileIterator HeapFile::end() const {
    // TODO pa1.5: implement
    return HeapFileIterator{pages.size(), pages.size(), pages};
}