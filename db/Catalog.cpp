#include <db/Catalog.h>
#include <db/TupleDesc.h>
#include <stdexcept>
#include <iostream>

using namespace db;

void Catalog::addTable(DbFile *file, const std::string &name, const std::string &pkeyField) {
    int id = file->getId(); // Assuming getId() returns the id of the file
    Table table(file, name, pkeyField);
    if (tablesById.find(id) != tablesById.end()) {
        tablesById.erase(id);
        tablesById.emplace(id, table);
        return;
    }
    tablesById.emplace(id, table);
    idsByName[name] = id;
    // TODO pa1.2: implement
}

int Catalog::getTableId(const std::string &name) const {
    // return idsByName.at(name);

    auto iter = idsByName.find(name);
    if(iter != idsByName.end())
        return iter->second;
    throw std::invalid_argument("Key not found");
    // TODO pa1.2: implement
}

const TupleDesc &Catalog::getTupleDesc(int tableid) const {
//    return tablesById.at(tableid).file->getTupleDesc();
    auto iter = tablesById.find(tableid);
    if(iter != tablesById.end())
        return iter->second.file->getTupleDesc();
    throw std::invalid_argument("Key not found");
    // TODO pa1.2: implement
}

DbFile *Catalog::getDatabaseFile(int tableid) const {
    //return tablesById.at(tableid).file;
    auto iter = tablesById.find(tableid);
    if(iter != tablesById.end())
        return iter->second.file;
    throw std::invalid_argument("Key not found");
    // TODO pa1.2: implement
}

std::string Catalog::getPrimaryKey(int tableid) const {
    //return tablesById.at(tableid).pkeyField;
    auto iter = tablesById.find(tableid);
    if(iter != tablesById.end())
        return iter->second.pkeyField;
    throw std::invalid_argument("Key not found");
    // TODO pa1.2: implement
}

std::string Catalog::getTableName(int id) const {
    //return tablesById.at(id).name;
    auto iter = tablesById.find(id);
    if(iter != tablesById.end())
        return iter->second.name;
    throw std::invalid_argument("Key not found");
    // TODO pa1.2: implement
}

void Catalog::clear() {
    tablesById.clear();
    idsByName.clear();
    // TODO pa1.2: implement
}
