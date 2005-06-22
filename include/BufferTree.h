/* $Id$
 *
 * I/O efficient Buffer Tree implementation
 *
 * Copyright (C) 2004 Oliver Baltzer <ob@baltzer.net>
 */
#ifndef __BUFFERTREE_H
#define __BUFFERTREE_H

// we need this to implement the emptying queue
#include <deque>
// we use this to register dummy leaves
#include <map>

#include <portability.h>
#include <ami_stream.h>
#include <ami_coll.h>
#include <ami_block.h>
// TPIE memory manager
#include <mm.h>

// stats about collections
#include <tpie_stats_coll.h>
#include <tpie_stats_stream.h>

// debugging output
#include <ostream>
#include <iostream>

#define CONST_USAGE 1024 * 1024
// const usage + 2092 is the minimum space requirements

// forward declarations
template <typename TKey, typename TElement, typename TCompare, typename
TKeyOfElement, typename BTECOLL>
class BufferTree;

template <typename TElement>
struct BufferTreeOperation;

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement>
struct OperationOrder;

#include <BufferTreeNode.h>
#include <BufferTreeLeaf.h>

/**
 * Configuration structure for the BufferTree.
 */
struct BufferTreeParameters
{
    /**
     * Memory limit for the buffer tree. (Default 50MB)
     */
    size_t maxMemory;

    /**
     * Number of OS blocks to be used to represent a leaf. (Default 4)
     */
    size_t leafBlockFactor;

    /**
     * Forces the maximum node fanout to the specific number. (Default 0)
     */
    size_t forceMaxNodeFanout;

    /**
     * Forces the minimum node fanout to a specific number.
     */
    size_t forceMinNodeFanout;

    /**
     * Forces the leaf capacity to a certain size. (Default 0)
     */
    size_t forceLeafCapacity;

    /**
     * Forces the maximum number of operations that can be buffered.
     * (Default 0)
     */
    size_t forceMaxBufferLength;

    BufferTreeParameters() : maxMemory(52428800), leafBlockFactor(4),
                             forceMaxNodeFanout(0),
                             forceMinNodeFanout(0), forceLeafCapacity(0),
                             forceMaxBufferLength(0) {}
};

/**
 * Defines the type of the buffer tree operation.
 */
enum BufferTreeOperationType { INSERT, FIND, DELETE };

/** Operation Status */
enum BufferTreeOperationStatus { SUCCESSFUL, FAILED };

/**
 * Call back functions for the buffer tree.
 */
template<typename TElement>
struct BufferTreeCallBacks
{
    void (*insertCallBack)(TElement el, bool status);
    void (*findCallBack)(TElement el, bool status);
    void (*deleteCallBack)(TElement el, bool status);
};

/**
 * Represents an instance of a buffer tree operation.
 */
template <typename TElement>
struct BufferTreeOperation
{
    /** stores the type of the operation */
    BufferTreeOperationType type;
    /** element affected by the operation
     *  XXX: FIND and DELETE only consider the key of the element not its
     *       payload
     */
    TElement element;

    /** sequence number to order the operations */
    unsigned long int sequenceNumber;
};

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement>
struct OperationOrder
    : public binary_function<const BufferTreeOperation<TElement>&,
                             const BufferTreeOperation<TElement>&,
                             bool>
{
    TKeyOfElement koe_;
    TCompare comp_;

    bool operator()(const BufferTreeOperation<TElement>& op1,
                    const BufferTreeOperation<TElement>& op2)
    {
        int res = comp_(koe_(op1.element), koe_(op2.element));
        return res < 0 ? true
                       : res == 0 ? op1.sequenceNumber < op2.sequenceNumber
                                  : false;
    }
};

/**
 * Implementation of an I/O efficient buffer tree.
 *
 * Template types
 *
 * TKey:
 *   datatype of an elements key
 *
 * TElement:
 *   datatype of an element stored in the tree including the key of type
 *   T_Key
 *
 * TCompare:
 *   a binary functor type to test the equality of two keys
 *
 * TKeyOfElement:
 *   unary functor type to access the key of an element
 */
template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL = BTE_COLLECTION>
class BufferTree
{
    private:
        typedef BufferTreeNode<TKey, TElement, TCompare,
                               TKeyOfElement, BTECOLL> T_BTN;
        typedef BufferTreeLeaf<TKey, TElement, TCompare,
                               TKeyOfElement, BTECOLL> T_BTL;
        typedef BufferTree<TKey, TElement, TCompare, TKeyOfElement,
                           BTECOLL> T_BT;

        // the bid of the root node
        AMI_bid rootBID_;

        // reference to the root node of the buffer tree
        T_BTN* rootNode_;

        // stores the collection of nodes in external memory
        AMI_collection_single<BTECOLL>* nodeCollection_;

        // stores the leaves in external memory
        AMI_collection_single<BTECOLL>* leafCollection_;

        // the amount of memory available for computation in number of
        // BufferTreeOperations
        size_t availableMemoryBytes_;

        // maximum length of a buffer should be the maximum number of
        // operations fitting into memory
        size_t maxBufferLength_;

        // the configuration parameters of the Buffer Tree
        BufferTreeParameters params_;

        // the maximum node fanout
        size_t maxNodeFanout_;
        size_t minNodeFanout_;

        // the leaf capacity
        size_t leafCapacity_;

        // the report stream
        AMI_STREAM<TElement>* reportBuffer_;

        // last used sequence number
        unsigned long int lastSequenceNumber_;

        // the queue for nodes whose buffer is full
        std::deque<AMI_bid> fullBufferQueue_;

        // a map containing the bid of a leaf node and the index within the
        // node where dummy leaves are starting
        std::map<AMI_bid, size_t> dummyLeafNodes_;

        BufferTreeCallBacks<TElement> callbacks_;

    public:
        typedef TElement T_BTElement;
        typedef TKey T_BTKey;
        typedef BufferTreeOperation<TElement> T_BTOperation;

        tpie_stats_stream stats;

        // constructs a new BufferTree and initializes the required
        // variables.
        BufferTree(const BufferTreeParameters& params,
                   const BufferTreeCallBacks<TElement>& callbacks);

        // releases all occupied resources
        ~BufferTree();

        // returns the size of main memory
        inline size_t getAvailableMemoryBytes()
        {
            return this->availableMemoryBytes_;
        }

        // return the available main memory in terms of
        // BufferTreeOperations
        inline size_t getAvailableMemoryOperations()
        {
            return this->availableMemoryBytes_
                    / sizeof(BufferTreeOperation<TElement>);
        }

        inline size_t getS() const
        {
            // compute the balancing constraints
            return (((((this->getMaxNodeFanout() - 1)/ 2 + 1)
                    - this->getMinNodeFanout()) + 1) - 1) / 2 + 1;
        }

        inline size_t getT() const
        {
            return (this->getMaxNodeFanout() / 2
                    - this->getMinNodeFanout()) + getS() - 1;
        }

        // return the node fanout
        inline size_t getMaxNodeFanout() const
            { return this->maxNodeFanout_; }
        inline size_t getMinNodeFanout() const
            { return this->minNodeFanout_; }
        inline size_t getLeafCapacity() const
            { return this->leafCapacity_; }
        inline size_t getMaxBufferLength() const
            { return this->maxBufferLength_; }

        // returns the collection in which the nodes are stored
        AMI_collection_single<BTECOLL>* getNodeCollection() const;
        // returns the collection in which the leaves are stored
        AMI_collection_single<BTECOLL>* getLeafCollection() const;

        // allows a node or a leaf to report an element
        int reportElement(const TElement& el) const;

        // inserts an element into the buffer tree
        void insertElement(const TElement& el);

        // deletes an element from the buffer tree
        void deleteElement(const TElement& el);

        // finds an element with a given key in the buffer
        void findElement(const TElement& el);

        /**
         * @brief Queues the given nodes to empty its buffer.
         *
         * @param bid the block ID of the node to be queued.
         */
        void queueFullBuffer(AMI_bid bid);

        /**
         * @brief Registers a leaf node that has dummy leaves.
         */
        void registerDummyLeaves(AMI_bid bid, size_t index);

        /**
         * @brief Processes the queue and calls the buffer emptying
         *        process for each node stored in the queue.
         */
        void emptyFullBuffers(bool force = false);

        /** rebalances the the given node and all its parents */
        T_BTN* rebalanceInsert(T_BTN* node);

        inline AMI_bid getRootBID() { return this->rootBID_; }

        void setRootBID(
            AMI_bid rootBID
            ) { this->rootBID_ = rootBID; return; }

        void report(T_BTOperation op, BufferTreeOperationStatus status);

        void forceBufferEmptying();
        void forceBufferEmptying(AMI_bid bid);

        void removeDummyLeaves();

        void printStats(ostream& os);

        long getIO();
};

#include <BufferTree_impl.h>

#endif
