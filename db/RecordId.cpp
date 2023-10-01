#include <db/RecordId.h>
#include <stdexcept>
#include <string>

using namespace db;

//
// RecordId
//

// TODO pa1.4: implement
RecordId::RecordId(const PageId *pid, int tupleno) {
    this->pageId=pid;
    this->tupleno=tupleno;

}

bool RecordId::operator==(const RecordId &other) const {
    return (this->pageId->getTableId()==other.getPageId()->getTableId()) && (this->pageId->pageNumber()==other.getPageId()->pageNumber()) &&(this->tupleno==other.getTupleno());
    // TODO pa1.4: implement
}

const PageId *RecordId::getPageId() const {
    return pageId;
    // TODO pa1.4: implement
}

int RecordId::getTupleno() const {
    return tupleno;
    // TODO pa1.4: implement
}

//
// RecordId hash function
//

std::size_t std::hash<RecordId>::operator()(const RecordId &r) const {
    return std::hash<std::string>{}(std::to_string(r.getPageId()->getTableId())+std::to_string(r.getPageId()->pageNumber())+std::to_string(r.getTupleno()));
    // TODO pa1.4: implement

}
