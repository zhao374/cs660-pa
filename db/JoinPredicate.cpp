#include <db/JoinPredicate.h>
#include <db/Tuple.h>
#include "db/IntField.h"
#include "db/StringField.h"

using namespace db;

JoinPredicate::JoinPredicate(int field1, Predicate::Op op, int field2) {
    this->index1 = field1;
    this->op = op;
    this->index2 = field2;
    // TODO pa3.1: some code goes here
}

bool JoinPredicate::filter(Tuple *t1, Tuple *t2) {
    // we only handle int and string here
    if (t1->getField(index1).getType() == Types::INT_TYPE) {
        IntField f1 = (IntField &&) t1->getField(this->index1);
        IntField f2 = (IntField &&) t2->getField(this->index2);

        if (f1.getType() == f1.getType()) {
            return f1.compare(this->op, &f2);
        } else {
            return false;
        }
    } else if (t1->getField(index1).getType() == Types::STRING_TYPE) {
        StringField f1 = (StringField &&) t1->getField(this->index1);
        StringField f2 = (StringField &&) t2->getField(this->index2);

        if (f1.getType() == f1.getType()) {
            return f1.compare(this->op, &f2);
        } else {
            return false;
        }
    }
}

int JoinPredicate::getField1() const {
    return index1;
}

int JoinPredicate::getField2() const {
    return index2;
}

Predicate::Op JoinPredicate::getOperator() const {
    return op;
}
