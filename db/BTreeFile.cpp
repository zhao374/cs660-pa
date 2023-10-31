#include <db/BTreeFile.h>
#include "db/Database.h"
#include <fcntl.h>

using namespace db;

BTreeLeafPage *BTreeFile::findLeafPage(TransactionId tid, PagesMap &dirtypages, BTreePageId *pid, Permissions perm,
                                       const Field *f) {
    // Fetch the page from buffer pool
    BTreePage *page = dynamic_cast<BTreePage *>(this->getPage(tid,dirtypages,pid,perm));
    if (pid->getType() == BTreePageType::LEAF) {
        return dynamic_cast<BTreeLeafPage *>(page);
    }
    BTreeInternalPage *inPage = dynamic_cast<BTreeInternalPage *>(this->getPage(tid,dirtypages,pid,perm));
    BTreeEntry *bEntry;
    for (BTreeEntry entry: *inPage) { // assuming BTreeInternalPage has begin and end iterators
        bEntry = &entry;
        if (!f || f->compare(Op::LESS_THAN_OR_EQ, entry.getKey())) {
            return this->findLeafPage(tid, dirtypages, entry.getLeftChild(), Permissions::READ_ONLY, f);
        }
    }
    return this->findLeafPage(tid, dirtypages, bEntry->getRightChild(), Permissions::READ_ONLY, f);
}


BTreeLeafPage *BTreeFile::splitLeafPage(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *page, const Field *field) {
    BTreeLeafPage* rPage = dynamic_cast<BTreeLeafPage*>(this->getEmptyPage(tid, dirtypages, BTreePageType::LEAF));
    int pNum = page->getNumTuples();
    int splitIdx = pNum / 2;
    if (pNum % 2 == 1) splitIdx += 1;
    auto it =page->rbegin();
    Tuple* curTuple = nullptr;
    // keep half records to the left and half records to the right
    for (int count = 0; count < splitIdx && it != page->rend(); ++count, ++it) {
        *curTuple = *it;
        page->deleteTuple(curTuple);
        rPage->insertTuple(curTuple);
    }
    const Field& field1 = (curTuple->getField(this->keyField));
    Field* upKey = const_cast<Field*>(&field1);
    // find parent
    BTreeInternalPage* parPage = dynamic_cast<BTreeInternalPage*>(
            this->getParentWithEmptySlots(tid, dirtypages, page->getParentId(), upKey)
    );
    BTreePageId pageId=page->getId();
    BTreePageId rpageId=rPage->getId();
    parPage->insertEntry(*new BTreeEntry(upKey,&pageId,&rpageId));
    rPage->setParentId(&parPage->getId());
    if (page->getRightSiblingId() != nullptr) {
        BTreeLeafPage* rSibling = dynamic_cast<BTreeLeafPage*>(
                this->getPage(tid, dirtypages, page->getRightSiblingId(), Permissions::READ_WRITE)
        );
        BTreePageId rid=rPage->getId();
        rSibling->setLeftSiblingId(&rid);
        BTreePageId srid=rSibling->getId();
        rPage->setRightSiblingId(&srid);
    }
    BTreePageId rid1=rPage->getId();
    page->setRightSiblingId(&rid1);
    BTreePageId lid=page->getId();
    rPage->setLeftSiblingId(&lid);
    if (field->compare(Op::LESS_THAN_OR_EQ, upKey)) return page;
    return rPage;
}

/**
 * Split an internal page to make room for new entries and split its parent page as needed to accommodate
 * a new entry. The new entry for the parent should have a key matching
 * the middle key in the original internal page being split (this key is "pushed up" to the parent).
 * The child pointers of the new parent entry should point to the two internal pages resulting
 * from the split. Update parent pointers as needed.
 *
 * Return the internal page into which an entry with key field "field" should be inserted
 *
 * @param tid - the transaction id
 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
 * @param page - the internal page to split
 * @param field - the key field of the entry to be inserted after the split is complete. Necessary to know
 * which of the two pages to return.
 * @see getParentWithEmptySlots
 * @see updateParentPointers
 *
 * @return the internal page into which the new entry should be inserted
 */

BTreeInternalPage *BTreeFile::splitInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                                Field *field) {
    BTreeInternalPage* rPage = dynamic_cast<BTreeInternalPage*>(this->getEmptyPage(tid, dirtypages, BTreePageType::INTERNAL));

    // feed the right page
    int pNum = page->getNumEntries();
    int middleNum = pNum / 2;
    if (pNum % 2 == 1) middleNum++;
    auto it = page->rbegin();
    BTreeEntry * curEntry = nullptr;
    for (int count = 1; count < middleNum && it != page->rend(); count++, ++it) {
        *curEntry = *it;
        page->deleteKeyAndRightChild(curEntry);
        rPage->insertEntry(*curEntry);
    }
    // delete middle key from the left page,
    auto middleEntry =  *(++it);
    page->deleteKeyAndRightChild(&middleEntry);

    // find parent
    BTreeInternalPage* parPage = dynamic_cast<BTreeInternalPage*>(
            this->getParentWithEmptySlots(tid, dirtypages, page->getParentId(), middleEntry.getKey())
    );

    BTreePageId pageId=page->getId();
    BTreePageId rpageId=rPage->getId();
    parPage->insertEntry(*new BTreeEntry(middleEntry.getKey(),&pageId,&rpageId));
    rPage->setParentId(&parPage->getId());

    this->updateParentPointers(tid, dirtypages, page);
    this->updateParentPointers(tid, dirtypages, rPage);

    if (field->compare(Op::LESS_THAN_OR_EQ, middleEntry.getKey())) return page;
    return rPage;
}

void BTreeFile::stealFromLeafPage(BTreeLeafPage *page, BTreeLeafPage *sibling, BTreeInternalPage *parent,
                                  BTreeEntry *entry, bool isRightSibling) {
    auto it = sibling->rbegin();
    if (isRightSibling) {
        it = sibling->begin();
    }

    int numToMove = (sibling->getNumTuples() - page->getNumTuples()) / 2;
    Tuple tup;
    for (int i = 0; i < numToMove; ++i) {

        tup = (++it).operator*();
        sibling->deleteTuple(&tup);
        page->insertTuple(&tup);
    }
    if (isRightSibling) {
        tup = (++it).operator*();
    }
    if (isRightSibling) {
        auto myField = sibling->begin().operator*();
        auto field = const_cast<db::Field*>(&myField.getField(keyField));
        entry->setKey(field);
    } else {
        auto myField = page->begin().operator*();
        auto field = const_cast<db::Field*>(&myField.getField(keyField));
        entry->setKey(field);
    }
    parent->updateEntry(entry);
    // pa2.4: implement (BONUS)
}

void BTreeFile::stealFromLeftInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                          BTreeInternalPage *leftSibling, BTreeInternalPage *parent,
                                          BTreeEntry *parentEntry) {
    int numToMove = (leftSibling->getNumEntries() - page->getNumEntries()) / 2;

    auto it = leftSibling->rbegin();
    auto pageIt = page->begin();
    BTreeEntry hub(parentEntry->getKey(), nullptr, (*(++pageIt)).getLeftChild());

    for (int i = 0; i < numToMove; ++i) {
        BTreeEntry left = *(++it);
        hub.setLeftChild(left.getRightChild());
        page->insertEntry(hub);
        hub = BTreeEntry(left.getKey(), nullptr, left.getRightChild());
        leftSibling->deleteKeyAndRightChild(&left);
    }
    parentEntry->setKey(hub.getKey());
    parent->updateEntry(parentEntry);
    updateParentPointers(tid, dirtypages, page);
    updateParentPointers(tid, dirtypages, leftSibling);

    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::stealFromRightInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                           BTreeInternalPage *rightSibling, BTreeInternalPage *parent,
                                           BTreeEntry *parentEntry) {
    int numToMove = (rightSibling->getNumEntries() - page->getNumEntries()) / 2;

    auto it = rightSibling->begin();
    auto pageIt = page->rbegin();
    BTreeEntry hub(parentEntry->getKey(), (*(++pageIt)).getRightChild(), nullptr);

    for (int i = 0; i < numToMove; ++i) {
        BTreeEntry right = *(++it);
        hub.setRightChild(right.getLeftChild());
        page->insertEntry(hub);
        hub = BTreeEntry(right.getKey(), right.getLeftChild(), nullptr);
        rightSibling->deleteKeyAndLeftChild(&right);
    }

    parentEntry->setKey(hub.getKey());
    parent->updateEntry(parentEntry);
    updateParentPointers(tid, dirtypages, page);
    updateParentPointers(tid, dirtypages, rightSibling);

}

void BTreeFile::mergeLeafPages(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *leftPage,
                               BTreeLeafPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    auto it = rightPage->begin();

    while (it != rightPage->end()) {
        Tuple tup = *(++it);
        rightPage->deleteTuple(&tup);
        leftPage->insertTuple(&tup);
    }

    if (rightPage->getRightSiblingId() != nullptr) {
        auto * page = dynamic_cast<BTreeLeafPage*>(this->getPage(tid, dirtypages, rightPage->getRightSiblingId(), Permissions::READ_WRITE));

        page->setLeftSiblingId((BTreePageId *) &leftPage->getId());
    }

    leftPage->setRightSiblingId(rightPage->getRightSiblingId());
    this->setEmptyPage(tid, dirtypages, rightPage->getId().pageNumber());
    this->deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

}

void BTreeFile::mergeInternalPages(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *leftPage,
                                   BTreeInternalPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    auto leftIt = leftPage->rbegin();
    auto rightIt = rightPage->begin();

    BTreeEntry entryInParent(parentEntry->getKey(), (*(++leftIt)).getRightChild(), (*(++rightIt)).getLeftChild());
    leftPage->insertEntry(entryInParent);

    rightIt = rightPage->begin();
    while (rightIt != rightPage->end()) {
        BTreeEntry entry = *(++rightIt);
        rightPage->deleteKeyAndLeftChild(&entry);
        leftPage->insertEntry(entry);
    }

    setEmptyPage(tid, dirtypages, rightPage->getId().pageNumber());
    updateParentPointers(tid, dirtypages, leftPage);
    deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

}

BTreeFile::BTreeFile(const char *fname, int key, const TupleDesc &td) : keyField(key), td(td) {
    fd = open(fname, O_RDWR | O_CREAT | O_APPEND | O_TRUNC, 0644);
    if (fd == -1) {
        throw std::runtime_error("open");
    }
    tableid = std::hash<std::string>{}(fname);
}