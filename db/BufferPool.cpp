#include <db/BufferPool.h>
#include <db/Database.h>

using namespace db;

// TODO pa1.3: implement
BufferPool::BufferPool(int numPages) {
    this->maxPage=numPages;
}

Page *BufferPool::getPage(const TransactionId &tid,const PageId *pid) {
    //tid是啥， lock是啥。他那个.h file没有implement啊，不知道transactionid是啥
    for(const auto &page:this->buffer){
        if(page->getId()==*pid){
            return page;
        }
    }
    Page *newPage= Database::getCatalog().getDatabaseFile(pid->getTableId())->readPage(*pid);
    buffer.push_back(newPage);

    if(buffer.size() >= maxPage)
        evictPage();

    return newPage;
    // TODO pa1.3: implement
}

void BufferPool::evictPage() {
    // do nothing for now
}
