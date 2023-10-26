#include <db/IndexPredicate.h>

using namespace db;

IndexPredicate::IndexPredicate(Op op, const Field *fvalue): op(op), fvalue(fvalue) {

    // TODO pa2.2: implement
}

const Field *IndexPredicate::getField() const {
    return fvalue;
    // TODO pa2.2: implement
    return {};
}

Op IndexPredicate::getOp() const {
    return op;
    // TODO pa2.2: implement
    return {};
}

bool IndexPredicate::operator==(const IndexPredicate &other) const {
    return op == other.op && *fvalue == *other.fvalue;
    // TODO pa2.2: implement
    return {};
}

std::size_t std::hash<IndexPredicate>::operator()(const IndexPredicate &ipd) const {
    // TODO pa2.2: implement
    std::hash<const Field *> ptr_hash;
    std::size_t h1 = std::hash<int>{}(static_cast<int>(ipd.getOp()));
    std::size_t h2 = ptr_hash(ipd.getField());
    return h1 ^ h2;
}
