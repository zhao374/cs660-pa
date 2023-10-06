#include <db/HeapFile.h>
#include <db/TupleDesc.h>
#include <db/Page.h>
#include <db/PageId.h>
#include <db/HeapPage.h>
#include <stdexcept>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
using namespace db;

//
// HeapFile
//

#include <iostream>
#include <sstream>
#include <iomanip>
#include <array>
unsigned int hashFunction(const char* str) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }

    return static_cast<int>(hash);
}


// TODO pa1.5: implement
HeapFile::HeapFile(const char *fname, const TupleDesc &td) : fname(fname) {
    this->tableId = hashFunction(fname);
    this->td = td;
    Database::getCatalog().addTable(this,fname);
    this->fname = fname;
}

int HeapFile::getId() const {
    return tableId;
    // TODO pa1.5: implement
}

const TupleDesc &HeapFile::getTupleDesc() const {
    return td;
    // TODO pa1.5: implement
}

Page *HeapFile::readPage(const PageId &pid) {
    int page_size=Database::getBufferPool().getPageSize();
    int offset=pid.pageNumber()*page_size;
    int fd = open(this->fname, O_RDONLY);
    uint8_t data[4096];
    ssize_t bytesRead = pread(fd, data,page_size, offset);
    HeapPageId hpid(pid.getTableId(),pid.pageNumber());
    HeapPage* heapPage = new HeapPage(hpid, data);
    return static_cast<Page*>(heapPage);

    // TODO pa1.5: implement
}



int HeapFile::getNumPages() {
    int fd = open(this->fname, O_RDONLY);
    return std::ceil(lseek(fd, 0, SEEK_END)/Database::getBufferPool().getPageSize());;
    // TODO pa1.5: implement
}

HeapFileIterator HeapFile::begin() const {
    // TODO find a way to get all the pages in a file and read them into tuples
    //std::vector<Tuple> myTuples;

    return HeapFileIterator{0, this->myTuples.size(), this->myTuples};
    // TODO pa1.5: implement
}

HeapFileIterator HeapFile::end() const {
    // TODO pa1.5: implement
    //std::vector<Tuple> myTuples;
    return HeapFileIterator{this->myTuples.size(), this->myTuples.size(), this->myTuples};
}