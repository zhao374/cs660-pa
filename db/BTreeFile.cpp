#include <db/BTreeFile.h>
#include "db/Database.h"

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
    for (int count = 0; count < splitIdx && it != page->rend(); ++count, ++it) {
        *curTuple = *it;
        page->deleteTuple(curTuple);
        rPage->insertTuple(curTuple);
    }
    const Field& field1 = (curTuple->getField(this->keyField));
    Field* upKey = const_cast<Field*>(&field1);
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

BTreeInternalPage *BTreeFile::splitInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                                Field *field) {

    // TODO pa2.3: implement
    return nullptr;
}

void BTreeFile::stealFromLeafPage(BTreeLeafPage *page, BTreeLeafPage *sibling, BTreeInternalPage *parent,
                                  BTreeEntry *entry, bool isRightSibling) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::stealFromLeftInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                          BTreeInternalPage *leftSibling, BTreeInternalPage *parent,
                                          BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::stealFromRightInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                           BTreeInternalPage *rightSibling, BTreeInternalPage *parent,
                                           BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::mergeLeafPages(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *leftPage,
                               BTreeLeafPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::mergeInternalPages(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *leftPage,
                                   BTreeInternalPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}
