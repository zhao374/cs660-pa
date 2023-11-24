#include <db/Join.h>

using namespace db;

Join::Join(JoinPredicate *p, DbIterator *child1, DbIterator *child2) {
    this->joinPred = p;
    this->child1 = child1;
    this->child2 = child2;
    this->outerTupleSize = child1->getTupleDesc().numFields();
    this->innerTupleSize = child2->getTupleDesc().numFields();
    this->mergedTD = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
    // TODO pa3.1: some code goes here
}

JoinPredicate *Join::getJoinPredicate() {
    return this->joinPred;
    // TODO pa3.1: some code goes here
}

std::string Join::getJoinField1Name() {
    return this->child1->getTupleDesc().getFieldName(this->joinPred->getField1());
    // TODO pa3.1: some code goes here
}

std::string Join::getJoinField2Name() {
    return this->child2->getTupleDesc().getFieldName(this->joinPred->getField2());
    // TODO pa3.1: some code goes here
}

const TupleDesc &Join::getTupleDesc() const {
    return this->mergedTD;
    // TODO pa3.1: some code goes here
}

void Join::open() {
    this->child1->open();
    this->child2->open();
    Operator::open();

    // TODO pa3.1: some code goes here
}

void Join::close() {
    this->child1->close();
    this->child2->close();
    Operator::close();
    // TODO pa3.1: some code goes here
}

void Join::rewind() {
    this->close();
    this->open();

    // TODO pa3.1: some code goes here
}

std::vector<DbIterator *> Join::getChildren() {
    return {child1, child2};
    // TODO pa3.1: some code goes here
}

void Join::setChildren(std::vector<DbIterator *> children) {
    child1 = children[0];
    child2 = children[1];
    // TODO pa3.1: some code goes here
}

std::optional<Tuple> Join::fetchNext() {
    Tuple innerTuple;
    while(true){
        switch(this->c1status){
            case PREPARING:
                if(this->child1->hasNext()){
                    this->outerTuple=this->child1->next();
                    this->c1status=READING;
                }else{
                    return {};
                }
                break;
            case READING:
                if(this->c2status==END){
                    if(this->child1->hasNext()){
                        this->outerTuple=this->child1->next();
                        this->child2->rewind();
                        this->c2status=PREPARING;
                    }else{
                        this->c1status=END;
                        return {};
                    }
                    continue;
                }
                break;
            case END:
                return {};

        }
        switch(this->c2status){
            case PREPARING:
                if(this->child2->hasNext()){
                    innerTuple=this->child2->next();
                    this->c2status=READING;
                }else{
                    return {};
                }
                break;
            case READING:
                if(this->child2->hasNext()){
                    innerTuple=this->child2->next();
                }else{
                    this->c2status=END;
                    continue;
                }
                break;
            case END:
                continue;

        }
        if(this->joinPred->filter(&(this->outerTuple),&innerTuple)){
            auto mergedTuple= Tuple(this->mergedTD);
            for(int i=0;i<this->outerTupleSize;++i){
                mergedTuple.setField(i,&outerTuple.getField(i));
            }
            for(int i=0;i<this->innerTupleSize;++i){
                mergedTuple.setField(i+outerTupleSize,&innerTuple.getField(i));
            }
            return mergedTuple;
        }
    }
    // TODO pa3.1: some code goes here
}
