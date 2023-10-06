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

    db::Database::getCatalog().addTable(this, fname);
    // Get the current working directory
    std::filesystem::path currentPath = std::filesystem::current_path();

    // Convert the path to a string and print it
    std::cout << "Current Working Directory: " << currentPath.string() << std::endl;


    // Open the .dat file in binary mode
    std::ifstream file(fname, std::ios::binary);

    if (!file.is_open()) {
        std::cerr << "Error opening file." << std::endl;
        return;
    }

    // Read data from the file into EXAMPLE_DATA
    uint8_t data[4096]; // Define your array
    file.read(reinterpret_cast<char*>(data), sizeof(data));

    // Check if the read was successful
    if (!file) {
        std::cerr << "Error reading file." << std::endl;
        return;
    }

    // Process the data in EXAMPLE_DATA
    for (int i = 0; i < sizeof(data); ++i) {
        // You can access the data in EXAMPLE_DATA[i] here
        std::cout << "Read data at index " << i << ": " << static_cast<int>(data[i]) << std::endl;
    }

    // Close the file when done
    file.close();

    HeapPage page(HeapPageId(tableId, 0) , data);
    this->pages.push_back(page);

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
    return &pages[pid.pageNumber()];
    // TODO pa1.5: implement
}

int HeapFile::getNumPages() {
    return pages.size();
    // TODO pa1.5: implement
}

HeapFileIterator HeapFile::begin() const {
    // TODO find a way to get all the pages in a file and read them into tuples
    std::vector<Tuple> myTuples;
    return HeapFileIterator{0, myTuples.size(), myTuples};
    // TODO pa1.5: implement
}

HeapFileIterator HeapFile::end() const {
    // TODO pa1.5: implement
    std::vector<Tuple> myTuples;
    return HeapFileIterator{myTuples.size(), myTuples.size(), myTuples};
}