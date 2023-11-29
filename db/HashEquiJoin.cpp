#include <db/HashEquiJoin.h>
#include "db/IntField.h"
#include "db/StringField.h"
#include <unordered_map>
using namespace db;

HashEquiJoin::HashEquiJoin(JoinPredicate p, DbIterator *child1, DbIterator *child2):joinPred(p) {
    this->child1=child1;
    this->child2 = child2;
    it = myTuples.begin();
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
    Operator::open();

    if (this->keyType == Types::INT_TYPE) {
        this->IntMap = std::unordered_map<int, std::vector<Tuple>>();
    } else if (this->keyType == Types::STRING_TYPE) {
        this->StrMap = std::unordered_map<std::string, std::vector<Tuple>>();
    }

    Tuple c1ReadTuple;

    while (this->child1->hasNext()) {
        c1ReadTuple = this->child1->next();

        if (this->keyType == Types::INT_TYPE) {
            IntField* key = (IntField *) &(c1ReadTuple.getField(this->joinPred.getField1()));
            if (key != nullptr) {
                if (this->IntMap.find(key->getValue()) != this->IntMap.end()) {
                    std::vector<Tuple> the_list = this->IntMap[key->getValue()];
                } else {
                    this->IntMap[key->getValue()] = std::vector<Tuple>();
                }
                this->IntMap[key->getValue()].push_back(c1ReadTuple);
            }
        }
    }

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
    if (it != this->IntMap[curKey->getValue()].end()) {
        return this->makeJoin();
    } else {
        while (this->child2->hasNext()) {
            this->tup2 = this->child2->next();

            if (this->keyType == Types::INT_TYPE) {
                curKey = (IntField *) &this->tup2.getField(this->joinPred.getField2());
                if (this->IntMap.find(curKey->getValue()) != this->IntMap.end()) {
                    it = this->IntMap[curKey->getValue()].begin();
                } else {
                    continue;
                }
            }

            if (it+1 == this->IntMap[curKey->getValue()].end()) {
                continue;
            }

            return this->makeJoin();
        }
    }
}

Tuple HashEquiJoin::makeJoin() {
    Tuple tup1 = *(it++);
    Tuple * mergedTuple = new Tuple(mergedTupleDesc);

    for (int i = 0; i < tup1Size; ++i) {
        mergedTuple->setField(i, &tup1.getField(i));
    }
    for (int i = 0; i < tup2Size; ++i) {
        mergedTuple->setField(i+ tup1Size, &tup2.getField(i));
    }
    return *mergedTuple;
}
