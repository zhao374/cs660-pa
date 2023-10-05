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
HeapFile::HeapFile(const char *fname, const TupleDesc &td) {
    this->tableId = hashFunction(fname);
    this->td = td;
    Database::getCatalog().addTable(this,fname);
    const size_t PAGE_SIZE = 4096;
    int fd = open(fname, O_RDONLY);
//    printf(reinterpret_cast<const char *>(fd));
    off_t offset = 0; // Offset from the beginning of the file
    int pagen=0;
    while (true) {
        uint8_t data[4096];
        ssize_t bytesRead = pread(fd, data, PAGE_SIZE, offset);
        if (bytesRead <= 0) {
            break;
        }
        pagen+=1;
        pages.push_back(HeapPage(HeapPageId(tableId,pagen),data));
        offset += bytesRead;
    }
    close(fd);
    std::cout << "Read " << pages.size() << " pages from the file." << std::endl;

    this->fname = fname;
    for(auto &page:pages){
        for(auto &tuple: page){
            myTuples.push_back(tuple);
        }
    }
    std::cout << myTuples.size() << std::endl;


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
    for(auto & page: pages){
        if(page.getId()==pid){
            return &page;
        }
    }
    return nullptr;
    // TODO pa1.5: implement
}



int HeapFile::getNumPages() {
    return pages.size();
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