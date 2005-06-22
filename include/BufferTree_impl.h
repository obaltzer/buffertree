/* $Id$
 *
 * Copyright (C) 2004 by Oliver Baltzer <ob@baltzer.net>
 */

/**
 * Constructor for the BufferTree creates a new block collection on disk in
 * which all nodes are stored. It also calculates the logical block factor
 * for determining the optimal number of children of a node.
 */
template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL>
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::BufferTree(const BufferTreeParameters& params,
             const BufferTreeCallBacks<TElement>& callbacks)
             : lastSequenceNumber_(0)
{

    // set the parameters
    this->params_ = params;
    this->callbacks_ = callbacks;
    MM_manager.set_memory_limit(this->params_.maxMemory);
    MM_manager.enforce_memory_limit();
    // the available main memory in bytes minus reservation for not yet
    // created objects
    availableMemoryBytes_ =
        MM_manager.memory_available() - CONST_USAGE;

    L_INFO("availableMemoryBytes: %d\n", availableMemoryBytes_);
    // available memory in operations
    size_t availableMemoryOperations =
        availableMemoryBytes_ / sizeof(T_BTOperation);
    size_t availableMemoryElements =
        availableMemoryBytes_ / sizeof(T_BTElement);

    // the buffer must fit into memory
    this->maxBufferLength_ = availableMemoryOperations;
    // force the maximum buffer length of so specified
    if(this->params_.forceMaxBufferLength > 0)
    {
        // make sure we do not screw up here
        assert(this->params_.forceMaxBufferLength
                    <= availableMemoryOperations);
        this->maxBufferLength_ = this->params_.forceMaxBufferLength;
    }

    L_INFO("sizeof(T_BTOperation): %d\n", sizeof(T_BTOperation));
    L_INFO("availableMemoryOperations (M): %d\n",
            availableMemoryOperations);
    L_INFO("maximum buffer length: %d\n", this->maxBufferLength_);

    // Compute how many elements a leaf can store and assume this number to
    // be B. This needs to be computed first such that it is possible to
    // determine the space requirements of a node in the tree.
    size_t blockSize =
        ((TPIE_OS_BLOCKSIZE() * this->params_.leafBlockFactor)
            - sizeof(BufferTreeLeafInfo))
        / sizeof(TElement);

    // the leaf's capacity is the blockSize
    this->leafCapacity_ = blockSize;
    // force capacity if specified
    if(this->params_.forceLeafCapacity > 0)
    {
        assert(this->params_.forceLeafCapacity <= blockSize);
        this->leafCapacity_ = this->params_.forceLeafCapacity;
    }

    L_INFO("Leaf Capacity: %d\n", this->leafCapacity_);
    L_INFO("sizeof(BufferTreeLeafInfo): %d\n", sizeof(BufferTreeLeafInfo));
    L_INFO("blockSize (B): %d\n", blockSize);

    // Compute m from available memory and B
    size_t availableMemoryBlocks = availableMemoryElements / blockSize;

    L_INFO("availableMemoryBlocks (m): %d\n", availableMemoryBlocks);

    // Compute the required memory for a tree node to store its maximum
    // span of m children. It needs to store m - 1 keys, m children of
    // type AMI_bid as well as some constant information.
    size_t nodeMem = (availableMemoryBlocks - 1) * sizeof(TKey)
                   + availableMemoryBlocks * sizeof(AMI_bid)
                   + sizeof(BufferTreeNodeInfo);

    L_INFO("nodeMem: %d\n", nodeMem);
    // Compute the logical block factor which matches the memory
    // requirements of a node best and then compute the actual fanout of
    // a node, which may be a little bit smaller than the ideal.
    size_t nodeBlockFactor = (nodeMem / TPIE_OS_BLOCKSIZE())
                           ? nodeMem / TPIE_OS_BLOCKSIZE()
                           : 1;
    L_INFO("nodeBlockFactor: %d\n", nodeBlockFactor);
    // Because the nodeBlockFactor my be result in slightly smaller nodes,
    // the actual fanout needs to be computed with the actual space a node
    // occupies. It is computed by the actual space available minus the
    // information structure and one link, divided by the size a key and a
    // link occupies. The one link we substracted before we add at the end
    // again.
    this->maxNodeFanout_ = (
        (nodeBlockFactor * TPIE_OS_BLOCKSIZE()
            - sizeof(BufferTreeNodeInfo) - sizeof(AMI_bid))
        / (sizeof(TKey) + sizeof(AMI_bid))) + 1;

    // force the node fanout if so specified
    if(this->params_.forceMaxNodeFanout > 0)
    {
        assert(this->params_.forceMaxNodeFanout <= this->maxNodeFanout_);
        this->maxNodeFanout_ = this->params_.forceMaxNodeFanout;
    }

    // set the min node fanout
    this->minNodeFanout_ = this->maxNodeFanout_ / 4;
    if(this->params_.forceMinNodeFanout > 0)
    {
        assert(this->params_.forceMinNodeFanout < this->maxNodeFanout_);
        this->minNodeFanout_ = this->params_.forceMinNodeFanout;
    }

    L_INFO("maxNodeFanout: %d\n", this->maxNodeFanout_);
    L_INFO("minNodeFanout: %d\n", this->minNodeFanout_);
    // create the collection for the nodes
    this->nodeCollection_ =
        new AMI_collection_single<BTECOLL>(nodeBlockFactor);

    // create the collection for the leaves
    this->leafCollection_ =
        new AMI_collection_single<BTECOLL>(this->params_.leafBlockFactor);

    // create the report buffer
    this->reportBuffer_ = new AMI_STREAM<TElement>("report");

    // create the root node
    this->rootNode_ = new T_BTN(this, 0, 0, LEAF);
    this->rootBID_ = this->rootNode_->bid();

    L_INFO("Available memory bytes: %d\n", getAvailableMemoryBytes());
    L_INFO("Available memory ops: %d\n", getAvailableMemoryOperations());
    return;
};

/**
 * @brief Removes the tree from external memory.
 *
 * Recurses down to the leaves and deletes one after the other. Then it
 * recursively delete the internal nodes of the tree and their buffers.
 */
template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL>
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::~BufferTree()
{
    int level = 0;

    L_DEBUG("Tree: going to destroy tree.\n");
    if(this->rootNode_ == NULL)
        this->rootNode_ = new T_BTN(this, this->rootBID_);

    T_BTN* node = this->rootNode_;

    bool done = false;
    // check if there is more to delete
    while(!done)
    {
        assert(node != NULL);
        // if the current node has children we have to recurse in those
        if(node->info()->nChildren != 0)
        {
            // if the current node is a leaf node, we just remove
            // the left-most leaf
            if(node->info()->type == LEAF)
            {
                // get the leaf
                T_BTL* leaf =
                    new T_BTL(this, node->lk[node->info()->nChildren - 1]);
                assert(leaf != NULL);
                leaf->persist(PERSIST_DELETE);
                delete leaf;
                node->info()->nChildren--;
            }
            // if the node is an internal node, we have to read its last
            // child, check if this is empty and if not recurse into it
            // if the last child is empty, we can delete it
            else
            {
                T_BTN* child =
                    new T_BTN(this, node->lk[node->info()->nChildren - 1]);
                if(child->info()->nChildren != 0)
                {
                    node->persist(PERSIST_PERSISTENT);
                    delete node;
                    assert(child != NULL);
                    node = child;
                    level++;
                }
                else
                {
                    child->persist(PERSIST_DELETE);
                    delete child;
                    node->info()->nChildren--;
                }
            }
        }
        // if the current node does not have any children, we have to pass
        // back to our parent
        else
        {
            T_BTN* parent = NULL;
            // if we are at the root we are done
            if(node->info()->parent == 0)
                done = true;
            else
            {
                // if not load our parent into memory
                parent = new T_BTN(this, node->info()->parent);
                parent->info()->nChildren--;
            }
            // delete the current node
            node->persist(PERSIST_DELETE);
            delete node;
            // set the new parent
            node = parent;
            level--;
        }
    }
    this->nodeCollection_->persist(PERSIST_DELETE);
    delete this->nodeCollection_;
    this->leafCollection_->persist(PERSIST_DELETE);
    delete this->leafCollection_;
    L_DEBUG("Tree: tree destroyed.\n");
}

/**
 * This method is called by the nodes to report the execution of a
 * Buffer Tree operation and its results.
 */
template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::report(T_BTOperation op, BufferTreeOperationStatus status)
{
    switch(op.type)
    {
        case INSERT:
            this->callbacks_.insertCallBack(
                op.element,
                status == SUCCESSFUL
            );
            break;
        case FIND:
            this->callbacks_.findCallBack(
                op.element,
                status == SUCCESSFUL
            );
            break;
        case DELETE:
            this->callbacks_.deleteCallBack(
                op.element,
                status == SUCCESSFUL
            );
            break;
    }
    return;
}

/**
 * Returns the pointer to the collection that stores the nodes.
 */
template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL>
AMI_collection_single<BTECOLL>*
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::getNodeCollection() const
{
    return this->nodeCollection_;
}

/**
 * Returns the pointer to the collection that stores the leaves.
 */
template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL>
AMI_collection_single<BTECOLL>*
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::getLeafCollection() const
{
    return this->leafCollection_;
}

/**
 * Report an element to the report buffer.
 */
template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL>
int
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::reportElement(const TElement& el) const
{
    AMI_err err;
    int retval = -1;
    if((err = this->_reportBuffer->write_item(el)) == AMI_ERROR_NO_ERROR)
    {
        L_DEBUG("Report: %d\n", el);
        retval = 0;
    }

    return retval;
}

/**
 * Insert an element into the buffer tree by creating a BufferTreeOperation
 * entity for this element and sending this BufferTreeOperation to the root
 * node of the tree.
 */
template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::insertElement(const TElement& el)
{
    BufferTreeOperation<TElement> op;
    op.type = INSERT;
    op.element = el;
    op.sequenceNumber = ++lastSequenceNumber_;
    this->rootNode_->insertOperation(op);
    this->emptyFullBuffers();
    return;
}

template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::deleteElement(const TElement& el)
{
    BufferTreeOperation<TElement> op;
    op.type = DELETE;
    op.element = el;
    op.sequenceNumber = ++lastSequenceNumber_;
    this->rootNode_->insertOperation(op);
    this->emptyFullBuffers();
    return;
}

template <typename TKey, typename TElement, typename TCompare,
          typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::findElement(const TElement& el)
{
    BufferTreeOperation<TElement> op;
    op.type = FIND;
    op.element = el;
    op.sequenceNumber = ++lastSequenceNumber_;
    this->rootNode_->insertOperation(op);
    this->emptyFullBuffers();
    return;
}

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::queueFullBuffer(AMI_bid bid)
{
    L_DEBUG("Queue full buffer for node: %d\n", bid);
    this->fullBufferQueue_.push_back(bid);
    return;
}

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::forceBufferEmptying()
{
    // add root to be emptied
    this->fullBufferQueue_.push_back(this->rootBID_);
    this->emptyFullBuffers(true);
    return;
}

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::forceBufferEmptying(AMI_bid bid)
{
    // add the first node that should be forced into the queue
    this->fullBufferQueue_.push_back(bid);
    this->emptyFullBuffers(true);
    return;
}

/**
 * Processes the queue containing the nodes which have full operation
 * buffers that need to be emptied.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::emptyFullBuffers(bool force)
{
    if(!this->fullBufferQueue_.empty())
    {
        // remember root BID
        this->rootBID_ = this->rootNode_->bid();
        // remove root node from memory
        delete this->rootNode_;
        this->rootNode_ = NULL;

        AMI_bid bid;
        while(!this->fullBufferQueue_.empty())
        {
            bid = this->fullBufferQueue_.front();
            L_DEBUG("Process full buffer: %d", bid);
            T_BTN* node = new T_BTN(this, bid);
            if(node->info()->type == INTERNAL)
                // if this node is internal, just pass the buffer to the
                // children
                node->emptyToNodes(force);
            else if(node->info()->type == LEAF)
            {
                // for a leaf node we have to at first empty to the leaves
                AMI_STREAM<TElement>* buffer = node->emptyToLeaves();
                // and now as long as the buffer is not NULL add a leaf at
                // a time and rebalance
                while(buffer != NULL)
                {

                    buffer = node->addLeaf(buffer);
                    T_BTN* sibling = this->rebalanceInsert(node);
                    if(sibling != NULL)
                    {
                        // if a leaf level sibling has been created, we
                        // need to add the rest of the stream to it, not
                        // to the current node.
                        delete node;
                        node = sibling;
                    }
                }
            }

            delete node;
            this->fullBufferQueue_.pop_front();
        }
        // load root node back into memory
        this->rootNode_ = new T_BTN(this, this->rootBID_);
        // remove dummy leaves if those exist
        this->removeDummyLeaves();
    }

    return;
}

/**
 * Calls the rebalancing after insert method for the specified node and
 * performs the same on all its parent node if a rebalancing occured.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>*
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::rebalanceInsert(T_BTN* node)
{
    // the leaf node sibling of the current node
    T_BTN* sibling = NULL;
    // TODO do not insert the sibling after the node, but before the node,
    // such that getting a reference to the sibling is not required.
    T_BTN* parent = node->rebalanceInsert(&sibling);

    while(parent != NULL)
    {
        T_BTN* temp_parent = parent->rebalanceInsert();
        // free the node
        delete parent;
        parent = temp_parent;
    }
    // return address to sibling or NULL of none was created
    return sibling;
}

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::registerDummyLeaves(AMI_bid bid, size_t index)
{
    L_DEBUG("registerDummyLeaves(): node = %d, pos = %d\n", bid, index);
    this->dummyLeafNodes_.insert(std::pair<AMI_bid, size_t>(bid, index));

    L_DEBUG("Register: ");
    std::map<AMI_bid, size_t>::iterator it2 =
        this->dummyLeafNodes_.begin();
    while(it2 != this->dummyLeafNodes_.end())
    {
        L_DEBUG("(%d, %d)", (*it2).first, (*it2).second);
        it2++;
    }
    L_DEBUG("\n");
    return;
}

/**
 * @brief Processes the queue of "dummy leaves" and rebalances the tree,
 * after removing leaves.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::removeDummyLeaves()
{
    if(!this->dummyLeafNodes_.empty())
    {
        // swap out the root node
        this->rootBID_ = this->rootNode_->bid();
        delete this->rootNode_;
        this->rootNode_ = NULL;

        // the leaf node which is the parent of the leaf
        T_BTN* childNode;
        // the parent node of the node that is currently processed
        T_BTN* parentNode;

        std::map<AMI_bid, size_t>::iterator it =
                this->dummyLeafNodes_.begin();
        // process all dummy leaves
        while(it != this->dummyLeafNodes_.end())
        {
            L_DEBUG("Register length: %d\n", this->dummyLeafNodes_.size());
            L_DEBUG("next dummy: node = %d, index = %d\n",
                    (*it).first, (*it).second);
            // reset
            childNode = NULL;
            parentNode = NULL;

            bool done = false;
            // as long as this leaf node has more dummy leaves remove
            // them
            while(!done)
            {
                // read the leaf if required
                if(childNode == NULL || childNode->bid() != (*it).first)
                {
                    if(childNode != NULL)
                        delete childNode;

                    // read the leaf node
                    childNode = new T_BTN(this, (*it).first);
                }
                // make sure the node is a leaf
                assert(childNode->info()->type == LEAF);

                // remove one dummy leaf from the end of the list of
                // children
                done = childNode->removeDummyLeaf((*it).second);

                // only do any rebalancing if this is not the root node
                if(childNode->info()->parent != 0)
                {
                // now get the parent node for rebalancing of the child
                // if required
                if(parentNode == NULL
                    || parentNode->bid() != childNode->info()->parent)
                {
                    if(parentNode != NULL)
                        delete parentNode;

                    parentNode =
                        new T_BTN(this, childNode->info()->parent);
                }
                // now rebalance until we cannot fuse or have reached the
                // root
                while(parentNode->rebalanceDelete(childNode))
                {
                    delete childNode;
                    // let the parent be the child
                    childNode = parentNode;
                    // now get the parent's parent
                    // if parent was the root the while loop would not have
                    // been entered, so we should be save here
                    parentNode =
                        new T_BTN(this, childNode->info()->parent);
                }

                // check if we have a new root
                if(parentNode->bid() == this->rootBID_
                    && parentNode->info()->nChildren == 1)
                {
                    // delete the old root
                    parentNode->persist(PERSIST_DELETE);
                    delete parentNode;
                    parentNode = NULL;

                    // set the new root
                    this->rootBID_ = childNode->bid();
                    childNode->info()->parent = 0;
                }
                // if not we should try sharing if required
                else
                    parentNode->shareNodes(childNode);
                }
            }
            it++;
        }
        this->dummyLeafNodes_.clear();
        if(parentNode != NULL)
            delete parentNode;
        if(childNode != NULL)
            delete childNode;
        parentNode = childNode = NULL;
        // load root node back into memory
        this->rootNode_ = new T_BTN(this, this->rootBID_);
    }
    return;
}

/*!
 * @brief writes the stats about the collections to the ostream
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::printStats(ostream& os)
{
    tpie_stats_collection nodes = this->nodeCollection_->stats();
    tpie_stats_collection leaves = this->leafCollection_->stats();

    os << "COLLECTION GETS: "
            << nodes.get(BLOCK_GET) + leaves.get(BLOCK_GET)
       << std::endl
       << "COLLECTION PUTS: "
            << nodes.get(BLOCK_PUT) + leaves.get(BLOCK_PUT)
       << std::endl
       << "COLLECTION NEWS: "
            << nodes.get(BLOCK_NEW) + leaves.get(BLOCK_NEW)
       << std::endl
       << "COLLECTION DELS: "
            << nodes.get(BLOCK_DELETE) + leaves.get(BLOCK_DELETE)
       << std::endl
       << "BUFFER BLOCK READ: "
            << this->stats.get(BLOCK_READ) << std::endl
       << "BUFFER BLOCK WRITE: "
            << this->stats.get(BLOCK_WRITE) << std::endl
       << "BUFFER BLOCK SEEK: "
            << this->stats.get(ITEM_SEEK) << std::endl
       << "STREAM OPEN: "
            << this->stats.get(STREAM_OPEN) << std::endl
       << "STREAM CLOSE: "
            << this->stats.get(STREAM_CLOSE) << std::endl
       << "STREAM CREATE: "
            << this->stats.get(STREAM_CREATE) << std::endl
       << "STREAM DELETE: "
            << this->stats.get(STREAM_DELETE) << std::endl;

}

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
long
BufferTree<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::getIO()
{
    tpie_stats_collection nodes = this->nodeCollection_->stats();
    tpie_stats_collection leaves = this->leafCollection_->stats();
    long retval = nodes.get(BLOCK_GET) + leaves.get(BLOCK_GET)
                + nodes.get(BLOCK_PUT) + leaves.get(BLOCK_PUT)
                + this->stats.get(BLOCK_READ)
                + this->stats.get(BLOCK_WRITE)
                + this->stats.get(ITEM_SEEK);
    return retval;
}
