/* $Id$ */
#ifndef __BUFFERTREENODE_H
#define __BUFFERTREENODE_H

// the buffer stream to buffer the operations
#include <ami_stream.h>
// the block that represents a node
#include <ami_block.h>

// STL stuff
#include <vector>
#include <deque>
#include <algorithm>
#include <functional>

#include <BufferTree.h>
#include <BufferTreeLeaf.h>
#include <StreamManager.h>
#include <Log.h>

#include <iostream>

enum BufferTreeNodeType { INTERNAL, LEAF };
/**
 * Contains additional information that is required when a persistent node
 * is loaded.
 */
struct BufferTreeNodeInfo
{
    /** Id of the stream containing the operation buffer. */
    StreamId sid;
    /** type of this node */
    BufferTreeNodeType type;
    /** number of children of the node */
    size_t nChildren;
    /** BID of the parent node */
    AMI_bid parent;
};

/**
 * Class representing a Buffer Tree node as a block in a collection of
 * blocks. The block stores the links to other nodes as AMI_bids as well as
 * the splitter elements of type TKey. Associated with each node is a
 * stream which buffers the operations applied to the subtree of this node.
 * The ID of the stream is stored in the BufferTreeNodeInfo structure
 * stored in the block as well.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
class BufferTreeNode : public AMI_block<TKey, BufferTreeNodeInfo, BTECOLL>
{
    private:
        typedef BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement,
                               BTECOLL> T_BTN;
        typedef BufferTreeLeaf<TKey, TElement, TCompare, TKeyOfElement,
                               BTECOLL> T_BTL;
        typedef BufferTree<TKey, TElement, TCompare, TKeyOfElement,
                               BTECOLL> T_BT;
        typedef BufferTreeOperation<TElement> T_BTOperation;

        /** reference to the stream containing the operation buffer */
        AMI_stream<T_BTOperation>* nodeBuffer_;

        /** reference to the buffer tree, to obtain global information. */
        T_BT* bufferTree_;

        /**
         * @brief appends an operation to the current buffer of the node.
         *
         * @param op the operation to be appended.
         */
        int appendOperation(const T_BTOperation& op);

        void distributeBufferToNodes(
            AMI_STREAM<T_BTOperation>* bufferStream,
            bool force = false
        );
        void sortBuffer(AMI_STREAM<T_BTOperation>* tempStream);

        void mergeBufferWithLeaves(
            AMI_STREAM<T_BTOperation>* bufferStream,
            AMI_STREAM<TElement>* mergeStream
        );
        void copyChildren(
            T_BTN* sibling,
            size_t outIdx,
            size_t inIdx,
            size_t n,
            size_t splitter
        );
        size_t findIndex(AMI_bid bid);
        void removeChild(size_t idx);
        void makeChild(AMI_bid bid);

    public:
        // Constructor for the node.
        BufferTreeNode(T_BT* bufferTree,
                       AMI_bid bid = 0,
                       AMI_bid parent = 0,
                       BufferTreeNodeType type = INTERNAL);

        // Destructor to release all obtained resources.
        ~BufferTreeNode();

        // opens the buffer stream
        int openBufferStream();

        // returns true if the length of nodeBuffer_ > M
        bool hasFullBuffer();


        AMI_STREAM<TElement>* emptyToLeaves();
        AMI_STREAM<TElement>* addLeaf(AMI_STREAM<TElement>* tempStream2);
        void emptyToNodes(bool force = false);

        T_BTN* rebalanceInsert(T_BTN** siblingRef = NULL);

        // inserts a new operation into the nodes buffer
        int insertOperation(const T_BTOperation& op);
        bool removeDummyLeaf(size_t index);
        size_t getBufferLength();
        bool rebalanceDelete(T_BTN* child);
        void shareNodes(T_BTN* childNode);
};

#include <BufferTreeNode_impl.h>

#endif

