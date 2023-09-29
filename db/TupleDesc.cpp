#include <db/TupleDesc.h>
#include <functional>
#include <sstream>

using namespace db;

//
// TDItem
//

std::size_t std::hash<TDItem>::operator()(const TDItem &r) const {
    return std::hash<std::string>{}(r.to_string());
    // TODO pa1.1: implement
}

//
// TupleDesc
//

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types) {
    size=0;
    for(int i=0;i<types.size();i++){
        size+= Types::getLen(types[i]);
        this->items.emplace_back(types[i], std::to_string(i));
    }
}

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types, const std::vector<std::string> &names) {
    size=0;
    for(int i=0; i<types.size(); i++){
        size+=Types::getLen(types[i]);
        this->items.emplace_back(types[i], names[i]);
    }
}

size_t TupleDesc::numFields() const {
    return this->items.size();
    // TODO pa1.1: implement
}

std::string TupleDesc::getFieldName(size_t i) const {
    return this->items[i].fieldName;
    // TODO pa1.1: implement
}

Types::Type TupleDesc::getFieldType(size_t i) const {
    return this->items[i].fieldType;
    // TODO pa1.1: implement
}

int TupleDesc::fieldNameToIndex(const std::string &fieldName) const {
    for(int i = 0; i < items.size(); i++) {
        if(items[i].fieldName == fieldName) return i;
    }

    return -1; // If field name is not found
    // TODO pa1.1: implement
}

size_t TupleDesc::getSize() const {
    return this->size;
    // TODO pa1.1: implement
}

std::vector<TDItem> TupleDesc::getItems() const {
    return this->items;
}

TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    std::vector<Types::Type> types;
    std::vector<std::string> names;
    for(auto& item : td1) {
        types.push_back(item.fieldType);
        names.push_back(item.fieldName);
    }
    for(auto& item : td2) {
        types.push_back(item.fieldType);
        names.push_back(item.fieldName);
    }
    return TupleDesc(types, names);
}

std::string TupleDesc::to_string() const {
    std::ostringstream oss;
    for(const auto& item : this->items)
        oss << item.to_string() << ", ";
    std::string result = oss.str();
    if(!result.empty())
        result.resize(result.size() - 2); // Remove the trailing ", "
    return result;
}

bool TupleDesc::operator==(const TupleDesc &other) const {
    return items == other.items;
}

TupleDesc::iterator TupleDesc::begin() const {
    return TDIterator{0, items.size(), items};
}

TupleDesc::iterator TupleDesc::end() const {
    return TDIterator{items.size(), items.size(), items};
}

std::size_t std::hash<db::TupleDesc>::operator()(const db::TupleDesc &td) const {
    std::size_t seed = 0;
    for(const auto& item : td)
        seed ^= std::hash<db::TDItem>{}(item) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return seed;
}
