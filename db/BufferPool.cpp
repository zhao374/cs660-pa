#include <db/BufferPool.h>
#include <random>
#include <db/Database.h>

using namespace db;

void BufferPool::evictPage() { // randomly choose a number
    // TODO pa2.1: implement
    int size = pages.size();
    std::mt19937 rng(std::random_device{}()); // Define a random number generator engine
    std::uniform_int_distribution<int> distribution(0, size); // Create a uniform distribution for the specified range
    int randomNumber = distribution(rng); // Generate a random number

    auto it = pages.begin();
    std::advance(it, randomNumber);

    flushPage(pages.begin()->first);
    pages.erase(pages.begin()->first);
}

void BufferPool::flushAllPages() {
    for (auto item: pages) {
        flushPage(item.first);
    }
    // TODO pa2.1: implement
}

void BufferPool::discardPage(const PageId *pid) {
    pages.erase(pid);
}

void BufferPool::flushPage(const PageId *pid) {
    DbFile * myFile = Database::getCatalog().getDatabaseFile(pid->getTableId());
    if (pages[pid]->isDirty()) {
        myFile->writePage(pages[pid]);
        pages[pid]->markDirty(std::nullopt);
    }

    // TODO pa2.1: implement
}

void BufferPool::flushPages(const TransactionId &tid) {
    for (auto item: pages) {
        if (item.second->isDirty() == tid) flushPage(item.first);
    }
    // TODO pa2.1: implement
}

void BufferPool::insertTuple(const TransactionId &tid, int tableId, Tuple *t) {
    DbFile * myFile = Database::getCatalog().getDatabaseFile(tableId);
    myFile->insertTuple(tid, *t);
}

void BufferPool::deleteTuple(const TransactionId &tid, Tuple *t) {
    DbFile * myFile = Database::getCatalog().getDatabaseFile(tid.operator int());
    myFile->deleteTuple(tid, *t);
    // TODO pa2.3: implement
}
