#include <db/Predicate.h>

using namespace db;

// TODO pa2.2: implement
Predicate::Predicate(int field, Op op, const Field *operand): field(field), op(op), operand(operand) {}

int Predicate::getField() const {
    return this->field;
    // TODO pa2.2: implement
    return {};
}

Op Predicate::getOp() const {
    return this->op;
    // TODO pa2.2: implement
    return {};
}

const Field *Predicate::getOperand() const {
    return this->operand;
    // TODO pa2.2: implement
    return {};
}

bool Predicate::filter(const Tuple &t) const {
    const Field* tupleField = &(t.getField(field));
    if (!tupleField) {
        // Field doesn't exist in the tuple
        return false;
    }
    return tupleField->compare(op,operand);
}
