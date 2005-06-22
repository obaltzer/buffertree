/* $Id$
 *
 * Copyright (C) 2004 by Oliver Baltzer <ob@baltzer.net>
 */

/**
 * Constructs a BufferTreeNode either based on the information stored in
 * the block with Id 'bid' in the collection 'coll' or by creating a new
 * block in the collection if 'bid == 0'.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::BufferTreeNode(T_BT* bufferTree,
                 AMI_bid bid, AMI_bid parent,
                 BufferTreeNodeType type)
    : AMI_block<TKey, BufferTreeNodeInfo, BTECOLL>(
        // the collection in which the block will be stored
        bufferTree->getNodeCollection(),
        // The fanout has been computed in the buffer tree alread.
        bufferTree->getMaxNodeFanout(),
        // The block Id to be used for the block. If this Id is 0 a new
        // block will be inserted into the collection.
        bid
    ), bufferTree_(bufferTree)
{
    // If we create a new node, also obtain a new stream Id for the buffer
    // node buffer.
    if(bid == 0)
    {
        L_DEBUG("Create new node: %d\n", this->bid());
        // Get the new stream Id.
        this->info()->sid = StreamId();
        StreamManager::getStreamId(this->info()->sid);
        // Initialize the number of children to none.
        this->info()->nChildren = 0;
        // set the type of the node
        this->info()->type = type;
        // set the parent
        this->info()->parent = parent;
        // set the block persistent
        this->persist(PERSIST_PERSISTENT);
    }
    nodeBuffer_ = NULL;
}

/**
 * The destructor closes the buffer stream and releases resources.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::~BufferTreeNode()
{

    // delete the buffer when the node is deleted
    if(this->per_ == PERSIST_DELETE)
    {
        L_DEBUG("node(%d): destroy node\n", this->bid());
        if(nodeBuffer_ == NULL)
            this->openBufferStream();
        this->nodeBuffer_->persist(PERSIST_DELETE);
    }

    // close the buffer stream
    if(nodeBuffer_ != NULL)
    {
        this->bufferTree_->stats.record(this->nodeBuffer_->stats());
        delete nodeBuffer_;
    }
}

/**
 * Inserts a BufferTreeOperation into the buffer of the current node and
 * recursively flushes the buffer if the buffer content exceeds M.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
int
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::insertOperation(const T_BTOperation& op)
{
    int retval = this->appendOperation(op);

    if(retval != -1 && this->hasFullBuffer())
    {
        this->bufferTree_->queueFullBuffer(this->bid());
        retval = 0;
    }
    return retval;
}

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
int
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::appendOperation(const T_BTOperation& op)
{
    AMI_err err;
    int retval = -1;

    // if the buffer stream is not open yet open it
    if(nodeBuffer_ == NULL)
        this->openBufferStream();

    assert(nodeBuffer_ != NULL);

    // TODO: seek to the end of the stream...we do not want to do this for
    // every element and we could probably optimize here
/*    if((err = nodeBuffer_->seek(nodeBuffer_->stream_len()))
      == AMI_ERROR_NO_ERROR)
    {
        nodeBuffer_->write_item(op);
        retval = 0;
    }
    */
    nodeBuffer_->write_item(op);
    retval = 0;

    return retval;
}

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
size_t
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::getBufferLength()
{
    // if the buffer stream is not open yet open it
    if(nodeBuffer_ == NULL)
        this->openBufferStream();
    return (size_t)nodeBuffer_->stream_len();
}


/**
 * Opens the buffer stream that is associated with the node.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
int
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::openBufferStream()
{
    int retval = 0;
    if(nodeBuffer_ == NULL)
    {
        char pathName[256] = "node";
        // create the pathname
        assert(this->info()->sid.toString(pathName, 256) != -1);
        L_DEBUG("Open stream: bid = %d path = %s\n", this->bid(), pathName);
        // open the buffer stream or create a new one
        nodeBuffer_ = new AMI_stream<T_BTOperation>(pathName);

        // check if the stream is valid
        assert(nodeBuffer_ != NULL);
        assert(nodeBuffer_->status() == AMI_STREAM_STATUS_VALID);
        nodeBuffer_->persist(PERSIST_PERSISTENT);
        nodeBuffer_->seek(nodeBuffer_->stream_len());
    }
    return retval;
}

/**
 * Returns true if the length of the buffer stream of this node is
 * larger than M.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
bool
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::hasFullBuffer()
{
    // if the buffer is NULL nothing has been written to it, and since
    // it was not full before, it cannot be full now.
    return (this->nodeBuffer_ != NULL
                && (size_t)nodeBuffer_->stream_len()
                        >= bufferTree_->getMaxBufferLength());
}

/**
 * @brief Distributes sorted buffer to the child node buffers.
 *
 * @param tempStream1 a pointer to the stream containing the sorted
 *                    operations from the buffer of the current node.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::distributeBufferToNodes(
        AMI_STREAM<T_BTOperation>* bufferStream,
        bool force
    )
{
    TKeyOfElement koe;
    TCompare comp;

    // pointer to the current operation in the sorted stream
    T_BTOperation* opPtr;
    bool streamDone =
        bufferStream->read_item(&opPtr) == AMI_ERROR_END_OF_STREAM;
    // the number of children of the current node
    size_t nChildren = this->info()->nChildren;

    // display this node's splitter elements
    L_DEBUG("Splitter for node %d are: ", this->bid());
    for(size_t i = 0; nChildren > 0 && i < nChildren - 1; i++)
        L_DEBUG("%d ", this->el[i]);
    L_DEBUG("\n");

    // iterate through each child of the current node (or create a child)
    // or stop when the stream has already been completely read
    // the current child
    T_BTN* child;
    for(size_t i = 0; i < nChildren && !streamDone; i++)
    {
        // load the child from disk
        child = new T_BTN(this->bufferTree_, this->lk[i]);

        // while the key of the current operation is smaller than the
        // splitter element, we write it to the buffer of the selected
        // child
        // Note: since there is no splitter key after the last child, we
        // add all remaining elements to it
        while(!streamDone
            && (i == nChildren - 1
                || comp(koe(opPtr->element), this->el[i]) < 0))
        {
            // append operation to child's buffer
            child->appendOperation(*opPtr);
            // load the next operation from the current stream
            streamDone =
                bufferStream->read_item(&opPtr) == AMI_ERROR_END_OF_STREAM;
        }
        // when we are done with the child we check the length of its
        // buffer an eventually append it to the buffer emptying queue
        if(child->nodeBuffer_ != NULL)
        {
            L_DEBUG("Child %d has length: %d\n",
                child->bid(), child->nodeBuffer_->stream_len());
        }

        // when we are done with the child we check the length of its
        // buffer an eventually append it to the buffer emptying queue
        if(child->hasFullBuffer() || force)
            this->bufferTree_->queueFullBuffer(child->bid());

        // throw child out of memory
        delete child;
    }
}

/**
 * @brief Executes the buffer emptying on an internal node.
 *
 * @param tempStream1 a pointer to the stream containing the sorted
 *                    operations from the buffer of the current node.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::emptyToNodes(bool force)
{
    TCompare comp;
    TKeyOfElement koe;

    // create a temporary stream where we merge the rest of the buffer
    // stream with the sorted list of operations
    AMI_STREAM<T_BTOperation>* tempStream1 =
        new AMI_STREAM<T_BTOperation>();
    // delete stream once we are done
    tempStream1->persist(PERSIST_DELETE);
    // open the buffer if not open
    if(this->nodeBuffer_ == NULL)
        this->openBufferStream();

    assert(this->nodeBuffer_ != NULL);
    // sort the buffer into tempStream
    this->sortBuffer(tempStream1);
    // distribute to the nodes
    this->distributeBufferToNodes(tempStream1, force);
    // reset this nodes buffer
    this->nodeBuffer_->truncate(0);
    this->nodeBuffer_->seek(0);
    delete tempStream1;

    return;
}

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::mergeBufferWithLeaves(
        AMI_STREAM<T_BTOperation>* bufferStream,
        AMI_STREAM<TElement>* mergeStream
    )
{
    TKeyOfElement koe;
    TCompare comp;

    // number of children of this leaf node
    size_t nChildren = this->info()->nChildren;
    // read the operation from the sorted buffer
    BufferTreeOperation<TElement>* opPtr;
    bool streamDone =
        bufferStream->read_item(&opPtr) == AMI_ERROR_END_OF_STREAM;

    // the current leaf we process
    T_BTL* leaf;
    for(size_t i = 0; i < nChildren; i++)
    {
        // we already have a leaf so load it
        leaf = new T_BTL(bufferTree_, this->lk[i]);

        // iterate through the elements of the leaf
        size_t nElements = leaf->info()->nElements;
        for(size_t j = 0; j < nElements;)
        {
            if(!streamDone
                && comp(koe(opPtr->element), koe(leaf->el[j])) < 0)
            {
                // the element in the buffer is smaller than the element
                // in the child
                switch(opPtr->type)
                {
                    case INSERT:
                        // The current element in the stream is smaller
                        // than the current element in the leaf and the
                        // current operation is insert. Thus the element
                        // from the stream should be inserted into the leaf
                        // before the element in the leaf.
                        mergeStream->write_item(opPtr->element);
                        this->bufferTree_->report(*opPtr, SUCCESSFUL);
                        break;
                    case DELETE:
                        // If the current element from the stream is
                        // smaller than the current element in the leaf and
                        // it represents a delete() function, the delete
                        // could not find the element to delete in the
                        // tree.
                        this->bufferTree_->report(*opPtr, FAILED);
                        break;
                    case FIND:
                        // same as for DELETE
                        this->bufferTree_->report(*opPtr, FAILED);
                        break;
                }

                // read the next element from the stream
                streamDone = bufferStream->read_item(&opPtr)
                    == AMI_ERROR_END_OF_STREAM;
            }
            else if(!streamDone
                && comp(koe(opPtr->element), koe(leaf->el[j])) == 0)
            {
                // the element in the buffer is the same as in the leaf
                switch(opPtr->type)
                {
                    case INSERT:
                        // When elements with same key occur, write the old
                        // one first.
                        mergeStream->write_item(leaf->el[j++]);
                        break;
                    case DELETE:
                        // we found something we want to delete, so do not
                        // anything an increment both counters
                        opPtr->element = leaf->el[j++];
                        // delete was successful
                        this->bufferTree_->report(*opPtr, SUCCESSFUL);
                        // read next operation from buffer
                        streamDone = bufferStream->read_item(&opPtr)
                            == AMI_ERROR_END_OF_STREAM;
                        break;
                    case FIND:
                        // Found the element and report it. Write the
                        // original element back to the child.
                        opPtr->element = leaf->el[j];
                        this->bufferTree_->report(*opPtr, SUCCESSFUL);
                        // read next operation from buffer
                        streamDone = bufferStream->read_item(&opPtr)
                            == AMI_ERROR_END_OF_STREAM;
                        break;
                }
            }
            else
            {
                /* std::cerr << "Write: " << opPtr->element.key
                          << " vs. "
                          << leaf->el[j].key
                          << " Result: "
                          << comp(koe(opPtr->element), koe(leaf->el[j]))
                          << " Stream: " << streamDone
                          << std::endl;
                */
                // the element from the buffer is larger, so write the
                // smaller element from the child first.
                mergeStream->write_item(leaf->el[j++]);
            }
        }
        // make the leaf block persistent
        leaf->persist(PERSIST_PERSISTENT);
        delete leaf;
    }
    // write the rest of the stream if all leaves have been processed
    // before the buffer was empty
    if(!streamDone)
    {
        do
        {
            switch(opPtr->type)
            {
                case INSERT:
                    // since all other elements in the output stream are
                    // smaller and all other operations in the buffer
                    // are larger just write the element
                    mergeStream->write_item(opPtr->element);
                    this->bufferTree_->report(*opPtr, SUCCESSFUL);
                    break;
                case DELETE:
                case FIND:
                    // cannot find the element
                    this->bufferTree_->report(*opPtr, FAILED);
                    break;
            }
        } while(bufferStream->read_item(&opPtr)
                            != AMI_ERROR_END_OF_STREAM);
    }
    return;
}

/**
 * Executes the buffer emptying on a leaf node.
 *
 * @param tempStream1 buffer of the node with its elements in sorted order
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
AMI_STREAM<TElement>*
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::emptyToLeaves()
{
    AMI_STREAM<TElement>* retval = NULL;

    // create a temporary stream where we merge the rest of the buffer
    // stream with the sorted list of operations
    AMI_STREAM<T_BTOperation>* tempStream1 =
        new AMI_STREAM<T_BTOperation>();
    // delete stream once we are done
    tempStream1->persist(PERSIST_DELETE);
    // open the buffer if not open
    if(this->nodeBuffer_ == NULL)
        this->openBufferStream();

    assert(this->nodeBuffer_ != NULL);
    L_DEBUG("Node (%d) has buffer size: %d\n", this->bid(),
            this->nodeBuffer_->stream_len());
    // sort the buffer into tempStream
    this->sortBuffer(tempStream1);

    // the number of children of this node
    size_t nChildren = this->info()->nChildren;

    // the tools to work with the elements
    TCompare comp;
    TKeyOfElement koe;

    // second temporary stream to merge the elements
    AMI_STREAM<TElement>* tempStream2 = new AMI_STREAM<TElement>();
    tempStream2->persist(PERSIST_DELETE);
    // merge the buffer with the leaves
    this->mergeBufferWithLeaves(tempStream1, tempStream2);

    // reset this nodes buffer
    this->nodeBuffer_->truncate(0);
    this->nodeBuffer_->seek(0);
    // and we do not need the sorted buffer anymore
    delete tempStream1;

    // reset the merged stream
    tempStream2->seek(0);
    // compute the number of blocks required to store the content of the
    // stream
    size_t requiredBlocks =
        tempStream2->stream_len() / bufferTree_->getLeafCapacity()
            + (tempStream2->stream_len()
                % bufferTree_->getLeafCapacity() ? 1 : 0);

    // distribute the elements from the stream into the leaves
    TElement* element;
    bool streamDone = false;
    L_DEBUG("node(%d) has %d children\n", this->bid(), nChildren);
    for(size_t i = 0; i < nChildren && !streamDone; i++)
    {
        // grab the leaf
        T_BTL* leaf = new T_BTL(bufferTree_, this->lk[i]);

        // reset the number of elements in the leaf to zero, which at the
        // same time marks our first "dummy leaf"
        size_t nElements = leaf->info()->nElements = 0;
        // add elements to the leaf until it is full or the merged stream
        // is done
        while(!streamDone && nElements < bufferTree_->getLeafCapacity())
        {
            // read next element from the stream
            streamDone = tempStream2->read_item(&element)
                    == AMI_ERROR_END_OF_STREAM;

            // if we have not reached the end of the stream, add the
            // element to the leaf
            if(!streamDone)
            {
                // update the splitter key with the key of the first
                // element that is going to be in the leaf block
                if(i != 0 && nElements == 0)
                    this->el[i - 1] = koe(*element);
                // add the element to the leaf
                leaf->el[nElements++] = *element;
            }
        }
        leaf->info()->nElements = nElements;

        // now check if we need to queue dummy elements
        if(streamDone)
        {
            // if this leaf is empty, it becomes a dummy element as
            // well as all following leaves
            if(leaf->info()->nElements == 0)
                this->bufferTree_->registerDummyLeaves(this->bid(), i);
            else if(i < this->info()->nChildren - 1)
                // otherwise only following leaves become dummies
                this->bufferTree_->registerDummyLeaves(this->bid(), i + 1);
        }

        // remove the leaf from memory
        leaf->persist(PERSIST_PERSISTENT);
        delete leaf;
    }

    if(!streamDone)
        // Return the reference to the stream, such that our caller can
        // call the add leaf method with it.
        retval = tempStream2;
    else
        // we are done
        delete tempStream2;

    return retval;
}

/**
 * Adds a leaf to the leaf node. This method requires a rebalanceInsert
 * call after it has been performed issued by the caller.
 *
 * @param tempStream2 list of elements that should be inserted to leaves
 * @return reference to the stream if more leaves need to be created or
           NULL if the stream has been completely processed.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
AMI_STREAM<TElement>*
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::addLeaf(AMI_STREAM<TElement>* tempStream2)
{
    TKeyOfElement koe;

    AMI_STREAM<TElement>* retval;

    // We have been called because in tempStream2 there may be more
    // elements that need to be inserted in a leaf.

    // so lets check
    size_t nChildren = this->info()->nChildren;
    TElement* element;
    if(tempStream2->read_item(&element) != AMI_ERROR_END_OF_STREAM)
    {
        //std::cerr << "\tAdd leaf to node: " << this->bid() << std::endl;
        // ok so there is an element in the stream to be inserted into a
        // leaf

        // update the splitter key with the key of the first element in
        // the list
        if(nChildren != 0)
            this->el[nChildren - 1] = koe(*element);


        // ...just lets create a leaf
        T_BTL* leaf = new T_BTL(bufferTree_, 0);

        bool streamDone = false;
        // write elements to the leaf until it is full or the stream is
        // empty
        leaf->el[0] = *element;
        // set the number of elements in the leaf
        size_t nElements = 1;
        while(nElements < bufferTree_->getLeafCapacity() && !streamDone)
        {
            streamDone = tempStream2->read_item(&element)
                            == AMI_ERROR_END_OF_STREAM;
            // write the element if we have not reached the end of the
            // string
            if(!streamDone)
                leaf->el[nElements++] = *element;
        }
        // update the leaf's number of elements
        leaf->info()->nElements = nElements;
        // add the leaf to my child links
        this->lk[nChildren++] = leaf->bid();
        // update the number of my children
        this->info()->nChildren = nChildren;
        // make leaf persistent
        leaf->persist(PERSIST_PERSISTENT);
        // remove leaf from memory
        delete leaf;

        // did we manage to process all the rest of the stream?
        if(streamDone)
        {
            // remove the stream from memory and delete it from disk
            delete tempStream2;
            // let caller know that we are done
            retval = NULL;
        }
        else
            // we need to keep calling this method but rebalance first
            retval = tempStream2;
    }
    else
    {
        // the stream was actually empty
        delete tempStream2;
        retval = NULL;
    }
    return retval;
}

/**
 * Rebalances the current node after an insert.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>*
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::rebalanceInsert(T_BTN** siblingRef)
{
    // returns the pointer to the parent node in memory to call its
    // rebalance method, or returns NULL if node was not rebalanced
    T_BTN* retval = NULL;

    // perform rebalancing if this node has a full link list
    if(this->info()->nChildren == bufferTree_->getMaxNodeFanout())
    {
        L_DEBUG("node(%d): node full -- rebalance after insert\n",
                this->bid());
        // contrary to what it says in the paper, I split the node first,
        // copy b/2 elements to the new node, remove the new node from
        // memory, update the links of my parent and return the address of
        // my parent to the caller

        // create this node's sibling which is of the same type as the
        // node itself
        T_BTN* sibling =
            new T_BTN(this->bufferTree_, 0, this->info()->parent,
                      this->info()->type);
        L_DEBUG("node(%d): new sibling is node(%d)\n",
                this->bid(), sibling->bid());
        // copy the n/2 rightmost children to the new node
        size_t nChildren = this->info()->nChildren;
        // determine the index of the first element to copy over to the
        // new sibling
        size_t lcount = (nChildren - 1) / 2 + 1;

        // temporary pointer to the child that gets copied to set the new
        // parent of the child
        T_BTN* child;
        for(int i = 0; i < nChildren / 2; i++)
        {
            sibling->lk[i] = this->lk[lcount];

            // we need to update the children's parent BIDs
            if(this->info()->type == INTERNAL)
            {
                child = new T_BTN(this->bufferTree_, sibling->lk[i]);
                child->info()->parent = sibling->bid();
                delete child;
            }
            // there are only nChildren - 1 spliter elements, make sure
            // that not too many get copied
            if(lcount < nChildren - 1)
                sibling->el[i] = this->el[lcount];
            lcount++;
        }
        L_DEBUG("node(%d) + node(%d): parent pointers updated\n",
                this->bid(), sibling->bid());

        // update new number of children for each node
        // my sibling gets the smaller half
        sibling->info()->nChildren = nChildren / 2;
        nChildren = this->info()->nChildren = (nChildren - 1) / 2 + 1;

        // now I need to get my parent to update its link list
        T_BTN* parentNode;
        if(this->bid() == this->bufferTree_->getRootBID())
        {
            // this is the special case when we just split the root and
            // thus have to create a new root
            L_DEBUG("node(%d): root node splitted -- create new\n",
                    this->bid());
            T_BTN* newRoot = new T_BTN(this->bufferTree_, 0, 0, INTERNAL);
            this->bufferTree_->setRootBID(newRoot->bid());
            newRoot->info()->parent = 0;
            newRoot->lk[0] = this->bid();
            newRoot->info()->nChildren = 1;
            this->info()->parent = newRoot->bid();
            parentNode = newRoot;
            L_DEBUG("Got new root: %d\n", parentNode->bid());
        }
        else
        {
            // get the parent node
            parentNode =
                new T_BTN(this->bufferTree_, this->info()->parent);
            L_DEBUG("Internal parent: %d\n", parentNode->bid());
        }

        // set our siblings parent
        sibling->info()->parent = parentNode->bid();
        // now we can get rid of our sibling
        // remember my siblings BID
        AMI_bid siblingBID = sibling->bid();

        // if the caller wants to have the address of the sibling,
        // we give it to him
        if(siblingRef != NULL)
            *siblingRef = sibling;
        else
            // otherwise remove my sibling from memory
            delete sibling;

        // find the link to me in my parents link list
        size_t myIndex;
        L_DEBUG("Those are my parents links: ");
        for(size_t i = 0; i < parentNode->info()->nChildren; i++)
            L_DEBUG("%d:%d", i, parentNode->lk[i]);
        L_DEBUG("\n");

        for(myIndex = 0; myIndex < parentNode->info()->nChildren
                   && parentNode->lk[myIndex] != this->bid(); myIndex++)

        // we should find our link
        L_DEBUG("This is: %d\n", this->bid());
        L_DEBUG("Parent Node is: %d\n", parentNode->bid());
        L_DEBUG("My index: %d\n", myIndex);
        assert(myIndex != parentNode->info()->nChildren);
        assert(parentNode->lk[myIndex] == this->bid());

        // now keep moving forward, insert my sibling and move everybody
        // following one position further.
        // my parent node should have at most lk.capacity() - 1 children
        // right now
        assert(parentNode->info()->nChildren
                    < bufferTree_->getMaxNodeFanout());
        myIndex++;
        assert(myIndex > 0);
        // the splitting key is the last spliting key in my truncated list
        // of splitting keys
        TKey key = this->el[nChildren - 1];
        AMI_bid sbid = siblingBID;
        while(myIndex < parentNode->info()->nChildren)
        {
            TKey temp_key = parentNode->el[myIndex - 1];
            AMI_bid temp_bid = parentNode->lk[myIndex];
            parentNode->lk[myIndex] = sbid;
            parentNode->el[myIndex - 1] = key;

            // remember old values
            key = temp_key;
            sbid = temp_bid;
            myIndex++;
        }
        parentNode->el[myIndex - 1] = key;
        parentNode->lk[myIndex] = sbid;
        // update my parent's child counter
        parentNode->info()->nChildren++;
        // return parent node for rebalancing
        retval = parentNode;
    }
    return retval;
}

template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::sortBuffer(AMI_STREAM<T_BTOperation>* tempStream)
{
    // 1. read M operations into memory and sort them
    OperationOrder<TKey, TElement, TCompare, TKeyOfElement> comp;
    // determine how many operations to read from the stream ... either
    // all of the stream of the first M elements
    size_t nOperations =
        (size_t)nodeBuffer_->stream_len()
            < this->bufferTree_->getMaxBufferLength()
            ? (size_t)nodeBuffer_->stream_len()
            : this->bufferTree_->getMaxBufferLength();

    // holds the in memory elements that have to be sorted
    std::vector<T_BTOperation> v(nOperations);
    // read the elements
    // the stream must be at least nOperations otherwise we would not have
    // called this function
    assert((size_t)nodeBuffer_->stream_len() >= nOperations);
    nodeBuffer_->seek(0);
    // temporary element
    T_BTOperation* opPtr;
    for(size_t i = 0; i < nOperations; i++)
    {
        nodeBuffer_->read_item(&opPtr);
        v[i] = *opPtr;
    }
    // now we need to sort that thing in place using STL stuff
    sort(v.begin(), v.end(), comp);

    bool streamDone =
        nodeBuffer_->read_item(&opPtr) == AMI_ERROR_END_OF_STREAM;
    // we definately have something in memory
    bool memDone = false;
    // memory index
    size_t i = 0;
    while(!streamDone || !memDone)
    {
        if(!streamDone && !memDone)
        {
            // we still read from the stream and the memory
            if(comp(v[i], *opPtr))
            {
                tempStream->write_item(v[i++]);
                memDone = i == nOperations;
            }
            else
            {
                tempStream->write_item(*opPtr);
                streamDone =
                    nodeBuffer_->read_item(&opPtr) == AMI_ERROR_END_OF_STREAM;
            }
        }
        // we only read from memory
        else if(streamDone)
        {
            for(; i < nOperations; i++)
                tempStream->write_item(v[i]);
            memDone = true;
        }
        // we only read from stream
        else if(memDone)
        {
            tempStream->write_item(*opPtr);
            while(nodeBuffer_->read_item(&opPtr) != AMI_ERROR_END_OF_STREAM)
                tempStream->write_item(*opPtr);
            streamDone = true;
        }
    }
    // reset pointer in temporary stream
    tempStream->seek(0);
}

/**
 * @brief Removes a dummy leaf from the end of the leaf list from this
 * leaf node.
 *
 * @return true if the last dummy leave has been removed from this node.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
bool
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::removeDummyLeaf(size_t index)
{
    // check if the index is smaller than our number of leaves
    L_DEBUG("removeDummyLeaf(): index = %d, nChildren = %d\n", index,
            this->info()->nChildren);

    if(index < this->info()->nChildren)
    {
        // get the BID of the last leaf in the list
        T_BTL* leaf = new T_BTL(this->bufferTree_,
                                this->lk[this->info()->nChildren - 1]);
        // remove the leaf from the collection
        leaf->persist(PERSIST_DELETE);
        delete leaf;
        this->lk[this->info()->nChildren - 1] = 0;
        this->info()->nChildren -= 1;
    }
    // since dummy leaves are always ordered behind the good leaves,
    // we can assume that once our index points behind the number of our
    // children, that we do not have a dummy leaf anymore
    return index == this->info()->nChildren;
}

/**
 * @brief Rebalances the specified child node.
 *
 * @param child the node that needs to be rebalanced
 * @return true if the recursion on the parent node can end
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
bool
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::rebalanceDelete(T_BTN* child)
{
    L_DEBUG("rebalanceDelete(): this.bid() = %d, child = %d\n",
            this->bid(), child->bid());
    // should we recurse?
    bool retval = false;

    // check if the child node is below the minimum number of children
    if(child->info()->nChildren < this->bufferTree_->getMinNodeFanout())
    {
        // our number of children
        size_t nChildren = this->info()->nChildren;
        // the index of the child in our link list
        size_t myIndex = this->findIndex(child->bid());

        assert(myIndex < nChildren);

        // the child we are going to balance with
        T_BTN* sibling = NULL;

        // the index in the link list of my right child (-1 no index)
        int siblingIdx = -1;

        // try to get a valid sibling to do fusion and stuff
        /*
        this->getValidSibling(
            child,
            &siblingIdx,
            &sibling,
            this->bufferTree_->getMinNodeFanout() + bufferTree_->getS()
        );
        */
        if(myIndex < nChildren - 1)
        {
            // try to balance with our right child if it exists
            // TODO the paper requires a buffer emptying on my sibling
            // here which I am not going to do, since I think the
            // buffer is empty already and it would get really messy since
            // I would need to throw this out of memory, such that the
            // updates on the link list stay consistent.
            sibling = new T_BTN(this->bufferTree_, this->lk[myIndex + 1]);
            L_DEBUG("Buffer length of right sibling: %d\n",
                    sibling->getBufferLength());

            // check if the child does not have to many children
            if(sibling->info()->nChildren <
                    this->bufferTree_->getMinNodeFanout()
                        + this->bufferTree_->getT() + 1)
                siblingIdx = myIndex + 1;
            else
                delete sibling;
        }

        // if the right sibling had too much children
        // its index is set to -1
        if(siblingIdx == -1 && myIndex > 0 && nChildren > 1)
        {
            // TODO the paper requires a buffer emptying on my sibling
            // here which I am not going to do, since I think the
            // buffer is empty already and it would get really messy since
            // I would need to throw this out of memory, such that the
            // updates on the link list stay consistent.
            sibling = new T_BTN(this->bufferTree_, this->lk[myIndex - 1]);
            L_DEBUG("Buffer length of right sibling: %d\n",
                    sibling->getBufferLength());
            // check if the child does not have to many children
            if(sibling->info()->nChildren <
                    this->bufferTree_->getMinNodeFanout()
                        + bufferTree_->getT() + 1)
                siblingIdx = myIndex - 1;
            else
                delete sibling;
        }

        if(siblingIdx == -1)
            // we cannot fuse nodes because both of my siblings have
            // too many children, such that we can only share
            retval = false;
        else
        {
            if(siblingIdx < myIndex)
            {
                // copy the children from sibling to child
                child->copyChildren(
                    sibling,
                    0, // start at the beginning
                    0, // insert on front --> shift original links
                    sibling->info()->nChildren, // all sibling's children
                    this->el[siblingIdx] // this is the splitter between
                );
                if(siblingIdx > 0)
                    // update the key in my parent
                    this->el[myIndex - 1] = this->el[siblingIdx - 1];
            }
            else
            {
                // copy the children from sibling to child
                child->copyChildren(
                    sibling,
                    0, // start at the beginning
                    child->info()->nChildren, // insert on end
                    sibling->info()->nChildren, // all sibling's children
                    this->el[myIndex] // this is the splitter between
                );
            }
            // delete the sibling and remove it from memory
            sibling->persist(PERSIST_DELETE);
            delete sibling;
            // now update my link list
            this->removeChild(siblingIdx);
            retval = true;
        }
    }
    // if this node is the root node do not recurse
    if(this->info()->parent == 0)
        retval = false;

    return retval;
}

/**
 * @brief Copies n children from sibling to this inserting at index
          inIdx starting at outIdx.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::copyChildren(
        T_BTN* sibling,
        size_t outIdx,
        size_t inIdx,
        size_t n,
        size_t splitter
    )
{
    assert(n + this->info()->nChildren <= bufferTree_->getMaxNodeFanout());

    // if the insert index is within the current list we have to make some
    // space
    if(inIdx < this->info()->nChildren)
    {
        // make space for children of the sibling
        // count from back
        for(ssize_t i = this->info()->nChildren - 1;
            i >= (ssize_t)inIdx; i--)
        {
            // copy links
            this->lk[i + n] = this->lk[i];
            // copy spliter
            if(i != 0)
                this->el[i + n - 1] = this->el[i - 1];
        }
        // set the splitter between both segments
        this->el[n - 1] = splitter;
    }
    else
        // set the splitter between both segments
        this->el[this->info()->nChildren - 1] = splitter;

    // start copying
    for(ssize_t i = 0; i < n; i++)
    {
        this->lk[inIdx + i] = sibling->lk[outIdx + i];
        // Update the child's parent link if the child is not a leaf.
        // We should only update children which are actually nodes.
        // FIXES: BT1
        if(this->info()->type == INTERNAL)
            this->makeChild(sibling->lk[outIdx + i]);
        if(i != 0)
            this->el[inIdx + i - 1] =
                sibling->el[outIdx + i - 1];
    }

    // realign left over children of sibling
    for(ssize_t i = outIdx; i < sibling->info()->nChildren - n; i++)
    {
        sibling->lk[i] = sibling->lk[i + n];
        if(i != 0)
            sibling->el[i - 1] = sibling->el[i + n - 1];
    }

    // set the new number of children
    this->info()->nChildren += n;
    sibling->info()->nChildren -= n;

    // TODO reorganize dummy leafs
    return;
}

/**
 * @brief Removes a link in the link list
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::removeChild(size_t idx)
{
    // update the links list
    for(size_t i = idx; i < this->info()->nChildren - 1; i++)
    {
        // shift link to left
        this->lk[i] = this->lk[i + 1];
        if(i != 0)
            // shift splitter to left
            this->el[i - 1] = this->el[i];
    }
    // decrement the number of children
    this->info()->nChildren--;
}

/**
 * @brief Update the childs parent link.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::makeChild(AMI_bid bid)
{
    T_BTN* child = new T_BTN(this->bufferTree_, bid);
    child->info()->parent = this->bid();
    delete child;
    return;
}

/**
 * @brief Tries to share nodes between children on the same level.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
void
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::shareNodes(T_BTN* child)
{
    // only share if the child actually has the capacities
    if(child->info()->nChildren < this->bufferTree_->getMinNodeFanout())
    {
        // get the child index
        size_t cIdx = this->findIndex(child->bid());
        assert(cIdx < this->info()->nChildren);

        T_BTN* sibling = NULL;

        size_t sIdx;

        // try the right child
        if(cIdx < this->info()->nChildren - 1)
        {
            sibling = new T_BTN(this->bufferTree_, this->lk[cIdx + 1]);
            sIdx = cIdx + 1;
            // check if my sibling has at least s children
            if(sibling->info()->nChildren - bufferTree_->getS()
                < this->bufferTree_->getMinNodeFanout())
            {
                delete sibling;
                sibling = NULL;
            }
        }
        // right child did not work, try left one
        if(sibling == NULL && cIdx > 0)
        {
            sibling = new T_BTN(this->bufferTree_, this->lk[cIdx - 1]);
            sIdx = cIdx - 1;
            // check if my sibling has at least s children
            if(sibling->info()->nChildren - bufferTree_->getS()
                < this->bufferTree_->getMinNodeFanout())
            {
                delete sibling;
                sibling = NULL;
            }
        }

        if(sibling != NULL && sIdx < cIdx)
        {
            // copy s children from the right side of the sibling
            child->copyChildren(
                sibling,
                sibling->info()->nChildren - bufferTree_->getS(),
                0,
                bufferTree_->getS(),
                this->el[cIdx - 1]
            );
            // set the new splitter between the child and the sibling
            // nodes which is basically the former splitter before the
            // first element that got copied to the child
            this->el[cIdx - 1] = sibling->el[sibling->info()->nChildren];
        }
        else if(sibling != NULL && sIdx > cIdx)
        {
            TKey tKey = sibling->el[bufferTree_->getS() - 1];
            // copy from left side
            child->copyChildren(
                sibling,
                0,
                child->info()->nChildren,
                bufferTree_->getS(),
                this->el[cIdx]
            );
            // set the new splitter between the child and the sibling
            // nodes which is splitter after the last element that got
            // copied to the child.
            this->el[cIdx] = tKey;
        }

        if(sibling != NULL)
            delete sibling;
    }
}

/**
 * @brief Returns the index of the given BID in the link list.
 */
template<typename TKey, typename TElement, typename TCompare,
         typename TKeyOfElement, typename BTECOLL>
size_t
BufferTreeNode<TKey, TElement, TCompare, TKeyOfElement, BTECOLL>
::findIndex(AMI_bid bid)
{
    // find the link to that child in our link list
    int retval;
    for(retval = 0; retval < this->info()->nChildren
            && this->lk[retval] != bid; retval++);
    // return retval == this->info()->nChildren ? -1 : retval;
    return retval;
}
