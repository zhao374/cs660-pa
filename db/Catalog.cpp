#include <db/Catalog.h>
#include <db/TupleDesc.h>
#include <stdexcept>

using namespace db;

void Catalog::addTable(DbFile *file, const std::string &name, const std::string &pkeyField) {
    int id = file->getId(); // Assuming getId() returns the id of the file
    Table table(file, name, pkeyField);
    tablesById.emplace(id, table);
    idsByName[name] = id;
    // TODO pa1.2: implement
}

int Catalog::getTableId(const std::string &name) const {
    auto iter = idsByName.find(name);
    if(iter != idsByName.end())
        return iter->second;
    // TODO pa1.2: implement
}

const TupleDesc &Catalog::getTupleDesc(int tableid) const {
    auto iter = tablesById.find(tableid);
    if(iter != tablesById.end())
        return iter->second.file->getTupleDesc();
    // TODO pa1.2: implement
}

DbFile *Catalog::getDatabaseFile(int tableid) const {
    auto iter = tablesById.find(tableid);
    if(iter != tablesById.end())
        return iter->second.file;
    // TODO pa1.2: implement
}

std::string Catalog::getPrimaryKey(int tableid) const {
    auto iter = tablesById.find(tableid);
    if(iter != tablesById.end())
        return iter->second.pkeyField;
    // TODO pa1.2: implement
}

std::string Catalog::getTableName(int id) const {
    auto iter = tablesById.find(id);
    if(iter != tablesById.end())
        return iter->second.name;
    // TODO pa1.2: implement
}

void Catalog::clear() {
    tablesById.clear();
    idsByName.clear();
    // TODO pa1.2: implement
}
