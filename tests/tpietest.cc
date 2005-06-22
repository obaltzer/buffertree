#define BTE_COLLECTION_IMP_MMAP
#define BTE_STREAM_IMP_MMAP
#define BTE_STREAM_UFS_BLOCK_FACTOR 4
#define BTE_STREAM_MMAP_BLOCK_FACTOR 4

#include <portability.h>
#include <ami_stream.h>
#include <ami_coll.h>
#include <ami_block.h>
#include <mm.h>

#include <iostream>

#define MM_LIMIT (50 * 1024 * 1024)

typedef int T_Key;

struct Element
{
    int key;
    int data;
};

enum BTOperationType { INSERT, DELETE, FIND };

template<typename T_Element>
struct BTOperation
{
    BTOperationType type;
    T_Element elem;
};
    
struct BTNodeInfo
{
    int sid;
    size_t size;
};

int main()
{
    MM_manager.set_memory_limit(MM_LIMIT);
    MM_manager.enforce_memory_limit();
    
    std::cout << "memory_available(): " << MM_manager.memory_available() 
              << std::endl;

    AMI_stream<BTOperation<Element> >* stream = 
        new AMI_stream<BTOperation<Element> >("stream");
    size_t B = stream->chunk_size();
    size_t M = MM_manager.memory_available() 
                / sizeof(BTOperation<Element>);
    size_t m = M / B;
    std::cout << "sizeof(BTOperation): " << sizeof(BTOperation<Element>) 
              <<  std::endl;
    std::cout << "B: " << B << std::endl;
    std::cout << "M: " << M << std::endl;
    std::cout << "m: " << m << std::endl;
    std::cout << "OS_BLOCKSIZE: " << TPIE_OS_BLOCKSIZE() << std::endl;
    std::cout << "B * sizeof(BTOperation): " 
              << (B * sizeof(BTOperation<Element>))
              << std::endl;
    std::cout << "sizeof(T_Key): " << sizeof(T_Key) << std::endl;
    std::cout << "sizeof(AMI_bid): " << sizeof(AMI_bid) << std::endl;
    std::cout << "sizeof(BTNodeInfo): " << sizeof(BTNodeInfo) << std::endl;
    // the memory a node requires to store references to m leaves
    size_t node_mem = (m - 1) * sizeof(T_Key) + (m * sizeof(AMI_bid))
                    + sizeof(BTNodeInfo);
    
    std::cout << "node_mem: " << node_mem << std::endl;
    // the number of OS_BLOCKS needed to store one node
    size_t node_lbf = (node_mem / TPIE_OS_BLOCKSIZE()) 
                    ? node_mem / TPIE_OS_BLOCKSIZE()
                    : 1;
    std::cout << "node_lbf: " << node_lbf << std::endl;
    // now compute the actual number of links to leaves given the chosen
    // block factor for a node
    size_t actual_links = (((node_lbf * TPIE_OS_BLOCKSIZE()) 
                         - sizeof(BTNodeInfo) - sizeof(AMI_bid))
                         / (sizeof(T_Key) + sizeof(AMI_bid))) + 1;
    std::cout << "actual_links: " << actual_links << std::endl;    
    AMI_collection<BTE_COLLECTION>* coll = 
        new AMI_collection<BTE_COLLECTION>("nodes.coll", 
                                           AMI_WRITE_COLLECTION, node_lbf);

    std::cout << "coll->block_size(): " << coll->block_size() << std::endl;
    
    AMI_block<T_Key, BTNodeInfo>* block = 
        new AMI_block<T_Key, BTNodeInfo>(coll, actual_links);
    std::cout << "block->lk.capacity(): " << block->lk.capacity() 
              << std::endl;
    std::cout << "block->el.capacity(): " << block->el.capacity() 
              << std::endl;
    std::cout << "memory_available(): " << MM_manager.memory_available() 
              << std::endl;
    // coll->persist(PERSIST_DELETE);
    // stream->persist(PERSIST_DELETE);
    delete stream;
    delete coll;
    return 0;
}
              
    
