#include <db/HashEquiJoin.h>
#include "db/IntField.h"
#include "db/StringField.h"
#include <unordered_map>
using namespace db;

HashEquiJoin::HashEquiJoin(JoinPredicate p, DbIterator *child1, DbIterator *child2):joinPred(p) {
    this->child1=child1;
    this->child2 = child2;
    this->tup1Size = child1->getTupleDesc().numFields();
    this->tup2Size = child2->getTupleDesc().numFields();
    this->mergedTupleDesc = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
    this->keyType = child1->getTupleDesc().getFieldType(p.getField1());
}

JoinPredicate *HashEquiJoin::getJoinPredicate() {
    return &joinPred;
}

const TupleDesc &HashEquiJoin::getTupleDesc() const {
    return mergedTupleDesc;
}

std::string HashEquiJoin::getJoinField1Name() {
    return child1->getTupleDesc().getFieldName(this->joinPred.getField1());
}

std::string HashEquiJoin::getJoinField2Name() {
    return child2->getTupleDesc().getFieldName(this->joinPred.getField2());

}

void HashEquiJoin::open() {
    this->child1->open();
    this->child2->open();

    if (this->keyType == Types::INT_TYPE) {
        this->IntMap = std::unordered_map<int, std::vector<Tuple>>();
    } else if (this->keyType == Types::STRING_TYPE) {
        this->StrMap = std::unordered_map<std::string, std::vector<Tuple>>();
    }

    Tuple c1ReadTuple;
    std::vector<Tuple> the_list;

    while (this->child1->hasNext()) {
        c1ReadTuple = this->child1->next();

        if (this->keyType == Types::INT_TYPE) {
            IntField* key = (IntField *) &(c1ReadTuple.getField(this->joinPred.getField1()));
            if (key != nullptr) {
                if (this->IntMap.find(key->getValue()) != this->IntMap.end()) {
                    the_list = this->IntMap[key->getValue()];
                } else {
                    the_list = std::vector<Tuple>();
                    this->IntMap[key->getValue()] = the_list;
                }
                the_list.push_back(c1ReadTuple);
            }
        } else {
            StringField* key = (StringField *) &(c1ReadTuple.getField(this->joinPred.getField1()));
            if (key != nullptr) {
                if (this->StrMap.find(key->getValue()) != this->StrMap.end()) {
                    the_list = this->StrMap[key->getValue()];
                } else {
                    the_list = std::vector<Tuple>();
                    this->StrMap[key->getValue()] = the_list;
                }
                the_list.push_back(c1ReadTuple);
            }
        }
    }
    Operator::open();

}

void HashEquiJoin::close() {
    this->child1->close();
    this->child2->close();
    this->IntMap.clear();
    this->StrMap.clear();
    // TODO pa3.1: some code goes here
}

void HashEquiJoin::rewind() {
    this->close();
    this->open();
}

std::vector<DbIterator *> HashEquiJoin::getChildren() {
    std::vector<DbIterator *> ans;
    ans.push_back(child1);
    ans.push_back(child2);
    return ans;
}

void HashEquiJoin::setChildren(std::vector<DbIterator *> children) {
    child1 = children[0];
    child2 = children[1];
    // TODO pa3.1: some code goes here
}

std::optional<Tuple> HashEquiJoin::fetchNext() {
    if (!myTuples.empty()) {
        return this->makeJoin();
    } else {
        while (this->child2->hasNext()) {
            this->tup2 = this->child2->next();

            if (this->keyType == Types::INT_TYPE) {
                IntField* key = (IntField *) &this->tup2.getField(this->joinPred.getField2());
                if (this->IntMap.find(key->getValue()) != this->IntMap.end()) {
                    myTuples = this->IntMap[key->getValue()];
                } else {
                    continue;
                }
            } else {
                StringField* key = (StringField *) &this->tup2.getField(this->joinPred.getField2());
                if (this->StrMap.find(key->getValue()) != this->StrMap.end()) {
                    myTuples = this->StrMap[key->getValue()];
                } else {
                    continue;
                }
            }

            if (myTuples.empty()) {
                continue;
            }

            return this->makeJoin();
        }
    }
}

Tuple HashEquiJoin::makeJoin() {
    Tuple tup1 = myTuples[indexOfTuples];
    indexOfTuples++;
    Tuple * mergedTuple = new Tuple(mergedTupleDesc);
    for (int i = 0; i < tup1Size; ++i) {
        mergedTuple->setField(i, &tup1.getField(i));
    }
    for (int i = 0; i < tup2Size; ++i) {
        mergedTuple->setField(i+ tup1Size, &tup2.getField(i));
    }
    return *mergedTuple;
}
