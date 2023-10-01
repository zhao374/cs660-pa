#include <db/Tuple.h>
#include "db/IntField.h"
#include "db/StringField.h"

using namespace db;

//
// Tuple
//

// TODO pa1.1: implement
Tuple::Tuple(const TupleDesc &td, RecordId *rid) {
    this->tupleDesc=td;
    this->recordId=rid;
    fields.resize(td.numFields());
}

const TupleDesc &Tuple::getTupleDesc() const {
    return this->tupleDesc;
    // TODO pa1.1: implement
}

const RecordId *Tuple::getRecordId() const {
    return this->recordId;
    // TODO pa1.1: implement
}

void Tuple::setRecordId(const RecordId *id) {
    recordId=id;
    // TODO pa1.1: implement
}

const Field &Tuple::getField(int i) const {
    return *fields[i];
    // TODO pa1.1: implement
}

void Tuple::setField(int i, const Field *f) {
    fields[i]= f;
    // TODO pa1.1: implement
}

Tuple::iterator Tuple::begin() const {
    return TupleIterator{0, fields.size(), fields};
    // TODO pa1.1: implement
}

Tuple::iterator Tuple::end() const {
    return TupleIterator{fields.size(), fields.size(), fields};
    // TODO pa1.1: implement
}

std::string Tuple::to_string() const {
    std::string result;
    for(const auto& field : fields)
        result += field->to_string() + ", ";
    if(!result.empty())
        result.pop_back();  // Remove the last space
    return result;
    // TODO pa1.1: implement
}
