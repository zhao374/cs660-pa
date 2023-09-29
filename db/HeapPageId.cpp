#include <db/HeapPageId.h>

using namespace db;

//
// HeapPageId
//

// TODO pa1.4: implement
HeapPageId::HeapPageId(int tableId, int pgNo) : tableId(tableId), pgNo(pgNo) {
}

int HeapPageId::getTableId() const {
    return this->tableId;
    // TODO pa1.4: implement
}

int HeapPageId::pageNumber() const {
    return this->pgNo;
    // TODO pa1.4: implement
}

bool HeapPageId::operator==(const PageId &other) const {
    if (const auto *otherPageId = dynamic_cast<const HeapPageId *>(&other)) {
        if(this->getTableId()==otherPageId->getTableId()&&this->pageNumber()==otherPageId->pageNumber()){
            return true;
        }else{
            return false;
        }

        // TODO pa1.4: implement
    }
    return false;
}
