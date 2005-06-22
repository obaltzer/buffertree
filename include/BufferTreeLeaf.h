#ifndef __BUFFERTREELEAF_H
#define __BUFFERTREELEAF_H

#include <ami_coll.h>
#include <BufferTree.h>

struct BufferTreeLeafInfo
{
    // the number of elements stored in the leaf
    size_t nElements;
    // a link to the previous leaf
    AMI_bid pref;
    // to the next leaf
    AMI_bid next;
};

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
class BufferTreeLeaf
    : public AMI_block<TElement, BufferTreeLeafInfo, BTECOLL>
{
    private:
        typedef BufferTree<TKey, TElement, TCompare, TKeyOfElement,
                           BTECOLL> T_BT;

        /** reference to the buffer tree, to obtain global information. */
        T_BT* bufferTree_;

    public:
        // Constructor for the leaf
        BufferTreeLeaf(T_BT* bufferTree, AMI_bid bid = 0);
        ~BufferTreeLeaf();
};

template<typename TKey, typename TElement, typename TCompare, typename TKeyOfElement, typename BTECOLL>
BufferTreeLeaf<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::BufferTreeLeaf(T_BT* bufferTree, AMI_bid bid)
    : AMI_block<TElement, BufferTreeLeafInfo, BTECOLL>(
        // the collection in which the block will be stored
        bufferTree->getLeafCollection(),
        // The fanout is 0 because a leaf does not have children
        0,
        // The block Id to be used for the block. If this Id is 0 a new
        // block will be inserted into the collection.
        bid
    ), bufferTree_(bufferTree)
{
    // If we create a new leaf, init some values
    if(bid == 0)
    {
        // Initialize the number of children to none.
        this->info()->nElements = 0;
        // set the block persistent
        this->persist(PERSIST_PERSISTENT);
    }
}

template<typename TKey, typename TElement, typename TCompare, typename TKeyOfElement, typename BTECOLL>
BufferTreeLeaf<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::~BufferTreeLeaf()
{
    /*
    if(this->per_ == PERSIST_DELETE)
        std::cerr << "leaf(" << this->bid() << "): destroy leaf"
                  << std::endl;
    */
}

#endif
