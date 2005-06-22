#define BTE_COLLECTION_IMP_MMAP
#define BTE_STREAM_IMP_UFS
#define BTE_STREAM_MMAP_BLOCK_FACTOR 1
#include <BufferTree.h>
#include <ami_btree.h>
#include <iostream>
#include <fstream>
#include <ostream>
#include <stdlib.h>
#include <mm.h>
#include <getopt.h>
#include <set>
#include <limits.h>
#include <sys/time.h>
#include <time.h>

#define MIN_MEM_REQ 1050670

struct Element
{
    long key;
    char data;

    friend ostream& operator<<(ostream& os, const Element& el);

};

ostream& operator<<(ostream& os, const Element& el)
{
    os << "(" << el.key << ", " << el.data << ")";
    return os;
}

class GetKey
{
    public:

        const long& operator()(const Element& e) { return e.key ; }
};

class CompareKey
{
    public:

        int operator()(const long& k1, const long& k2)
        {
            return k1 < k2 ? -1 : k1 == k2 ? 0 : 1;
        }
};

enum TestType
{
    BUFFERTREE = 0,
    BTREE
};

const char* typeNames[] = {"buffer", "btree"};

struct TestParameters
{
    size_t minLeafCapacity;
    size_t maxLeafCapacity;
    size_t minNodeFanout;
    size_t maxNodeFanout;
    size_t bufferLength;
    size_t blockFactor;
    size_t maxMemory;
    bool printParameters;
    TestType type;
    bool enableCache;
    TestParameters() :
        minLeafCapacity(0),
        maxLeafCapacity(0),
        minNodeFanout(0),
        maxNodeFanout(0),
        bufferLength(0),
        blockFactor(4),
        maxMemory(33554432),
        printParameters(true),
        type(BUFFERTREE),
        enableCache(false) {}
};

struct BenchmarkInfo
{
    long nIOs;
    double wallTime;

    BenchmarkInfo() : nIOs(0l), wallTime(0.0f) {}
};

typedef BufferTree<int, Element, CompareKey, GetKey> T_BUT;
typedef AMI_btree<int, Element, CompareKey, GetKey> T_BT;

typedef BufferTreeCallBacks<Element> T_BUTC;

void reportInsert(Element el, bool status)
{
    std::cerr << "Insert (" << el.key << ", " << el.data << "): "
              << (status ? "successful" : "failed") << std::endl;
    return;
}

void reportDelete(Element el, bool status)
{
    std::cerr << "Delete (" << el.key << ", " << el.data << "): "
              << (status ? "successful" : "failed") << std::endl;
    return;
}

void reportFind(Element el, bool status)
{
    std::cerr << "Find (" << el.key << ", " << el.data << "): "
              << (status ? "successful" : "failed") << std::endl;
    return;
}

void printBTreeParameters(ostream& os, T_BT* bt)
{
    const AMI_btree_params& params = bt->params();
    os  << "Size of Element: " << sizeof(Element)
            << std::endl
        << "Min Leaf Capacity: " << params.leaf_size_min
            << std::endl
        << "Max Leaf Capacity: " << params.leaf_size_max
            << std::endl
        << "Min Fanout: " << params.node_size_min
            << std::endl
        << "Max Fanout: " << params.node_size_max
            << std::endl;
}

void printBufferTreeParameters(ostream& os, T_BUT* bt)
{
    os  << "Size of Element: " << sizeof(T_BUT::T_BTElement)
            << std::endl
        << "Size of Operation: " << sizeof(T_BUT::T_BTOperation)
            << std::endl
        << "Available Memory: " << bt->getAvailableMemoryBytes()
            << std::endl
        << "Available Memory Blocks: "
            << bt->getAvailableMemoryBytes() / bt->getLeafCapacity()
            << std::endl
        << "Buffer length: " << bt->getMaxBufferLength()
            << std::endl
        << "Leaf Capacity: " << bt->getLeafCapacity()
            << std::endl
        << "Min Fanout: " << bt->getMinNodeFanout()
            << std::endl
        << "Max Fanout: " << bt->getMaxNodeFanout()
            << std::endl;
}

void printBTreeStats(ostream& os, T_BT* bt)
{
    const tpie_stats_tree& stats = bt->stats();

    os << "COLLECTION GETS: "
            << stats.get(NODE_READ) + stats.get(LEAF_READ)
       << std::endl
       << "COLLECTION PUTS: "
            << stats.get(NODE_WRITE) + stats.get(LEAF_WRITE)
       << std::endl
       << "COLLECTION NEWS: "
            << stats.get(NODE_CREATE) + stats.get(LEAF_CREATE)
       << std::endl
       << "COLLECTION DELS: "
            << stats.get(NODE_DELETE) + stats.get(LEAF_DELETE)
       << std::endl;
}

long getBTreeIO(T_BT* tree)
{
    const tpie_stats_tree& stats = tree->stats();
    long retval = stats.get(NODE_READ) + stats.get(LEAF_READ)
                + stats.get(NODE_WRITE) + stats.get(LEAF_WRITE);
    return retval;
}

int doBTreeTest(ostream& os, ifstream* inFile, long nOperations,
                TestParameters& params, BenchmarkInfo& info)
{
    AMI_btree_params treeParams;
    treeParams.leaf_size_min = params.minLeafCapacity;
    treeParams.leaf_size_max = params.maxLeafCapacity;
    treeParams.node_size_min = params.minNodeFanout;
    treeParams.node_size_max = params.maxNodeFanout;
    treeParams.leaf_block_factor = params.blockFactor;
    treeParams.node_block_factor = params.blockFactor;
    if(params.enableCache)
    {
        treeParams.leaf_cache_size = 5;
        treeParams.node_cache_size = 10;
    }
    else
    {
        treeParams.leaf_cache_size = 0;
        treeParams.node_cache_size = 0;
    }

    int memUsed = 0;

    T_BT* tree = new T_BT(treeParams);
    if(params.printParameters)
    {
        printBTreeParameters(os, tree);
        params.printParameters = false;
    }

    typedef std::set<int> T_KeySet;
    T_KeySet keys;

    std::cerr << "Running B-Tree test with " << nOperations
              << std::endl;
    std::cerr << "Ops: \x1b[s";
    struct timeval time;
    gettimeofday(&time, NULL);
    double begin = (double)time.tv_sec + (double)time.tv_usec / 1000000.0f;
    char c;
    for(long i = 0; i < nOperations; i++)
    {
        // reading operation code
        inFile->read((char*)&c, sizeof(char));
        Element e;
        // reading key
        inFile->read((char*)&e.key, sizeof(long));
        if(c == 'i')
        {
            e.data = 'T';
            tree->insert(e);
            //std::cerr << "insert(" << e.key << ")" << std::endl;
        }
        else if(c == 'd')
        {
            if(tree->erase(e.key))
            {
                //std::cerr << "delete(" << e.key << "): ok"
                //          << std::endl;
            }
            else
            {
                //std::cerr << "delete(" << e.key << "): failed"
                //          << std::endl;
            }
        }
        else if(c == 'f')
        {
            if(tree->find(e.key, e))
            {
                //std::cerr << "find(" << e.key << "): ok"
                //          << std::endl;
            }
            else
            {
                //std::cerr << "find(" << e.key << "): failed"
                //         << std::endl;
            }
        }

        if(MM_manager.memory_used() > memUsed)
            memUsed = MM_manager.memory_used();

        if(i % 1000 == 0)
        {
            std::cerr << "\x1b[u" << i;
            std::cerr.flush();
        }
    }
    gettimeofday(&time, NULL);
    double end = (double)time.tv_sec + (double)time.tv_usec / 1000000.0f;
    std::cerr << std::endl;
    // os << "Memory used: " << memUsed << std::endl;
    tree->persist(PERSIST_PERSISTENT);
    // printBTreeStats(os, tree);
    info.nIOs = getBTreeIO(tree);
    info.wallTime = end - begin;
    delete tree;
    return 0;
}

int doBufferTreeTest(ostream& os, ifstream* inFile, long nOperations,
                     TestParameters& params, BenchmarkInfo& info)
{
    // setting up callbacks
    T_BUTC callbacks;
    callbacks.insertCallBack = &reportInsert;
    callbacks.findCallBack = &reportFind;
    callbacks.deleteCallBack = &reportDelete;

    int memUsed = 0;

    // create the tree
    BufferTreeParameters treeParams;
    treeParams.forceLeafCapacity = params.maxLeafCapacity;
    treeParams.forceMinNodeFanout = params.minNodeFanout;
    treeParams.forceMaxNodeFanout = params.maxNodeFanout;
    treeParams.forceMaxBufferLength = params.bufferLength;
    treeParams.leafBlockFactor = params.blockFactor;
    treeParams.maxMemory = params.maxMemory;

    T_BUT* tree = new T_BUT(treeParams, callbacks);
    // print the tree parameters only once
    if(params.printParameters)
    {
        printBufferTreeParameters(os, tree);
        params.printParameters = false;
    }

    std::cerr << "Running Buffer Tree test with " << nOperations
              << std::endl;
    std::cerr << "Ops: \x1b[s";
    struct timeval time;
    gettimeofday(&time, NULL);
    double begin = (double)time.tv_sec + (double)time.tv_usec / 1000000.0f;
    char c;
    for(long i = 0; i < nOperations; i++)
    {
        // read the operation
        inFile->read((char*)&c, sizeof(char));
        Element e;
        // read the key
        inFile->read((char*)&e.key, sizeof(long));
        if(c == 'i')
        {
            e.data = 'T';
            tree->insertElement(e);
            // std::cerr << "insert(" << e.key << ")" << std::endl;
        }
        else if(c == 'd')
            tree->deleteElement(e);
        else if(c == 'f')
            tree->findElement(e);

        if(MM_manager.memory_used() > memUsed)
            memUsed = MM_manager.memory_used();
        if(i % 1000 == 0)
        {
            std::cerr << "\x1b[u" << i;
            std::cerr.flush();
        }
    }
    tree->forceBufferEmptying();
    gettimeofday(&time, NULL);
    double end = (double)time.tv_sec + (double)time.tv_usec / 1000000.0f;
    info.nIOs = tree->getIO();
    info.wallTime = end - begin;
    std::cerr << std::endl;
    // tree->printStats(os);
    // os << "Memory used: " << memUsed << std::endl;
    delete tree;

    #if 0
    for(int i = 0; i < 1000; i++)
    {
        if(i % 100000 == 0)
            std::cout << "..........Ops: " << i << std::endl;
        int r1 = (int)(((float)rand() / RAND_MAX) * 10);
        int r2 = (int)(((float)rand() / RAND_MAX) * 3);
        char c1 = 65 + (char)(((float)rand() / RAND_MAX) * 27);
        Element e;
        e.key = r1;
        strcpy(e.data, "Hello");
        t->insertElement(e);
        /*
        if(r2 == 0)
            t->insertElement(e);
        else if(r2 == 1)
            t->deleteElement(e);
        else if(r2 == 2)
            t->findElement(e);
        */
    }
    for(int i = 0; i < 1000; i++)
    {
        if(i % 100000 == 0)
            std::cout << "..........Ops: " << i << std::endl;
        int r1 = (int)(((float)rand() / RAND_MAX) * 10);
        int r2 = (int)(((float)rand() / RAND_MAX) * 3);
        char c1 = 65 + (char)(((float)rand() / RAND_MAX) * 27);
        Element e;
        e.key = r1;
        strcpy(e.data, "Hello");
        t->deleteElement(e);
        /*
        if(r2 == 0)
            t->insertElement(e);
        else if(r2 == 1)
            t->deleteElement(e);
        else if(r2 == 2)
            t->findElement(e);
        */
    }
#endif
#if 0
    for(int i = 0; i < 300; i++)
    {
        Element e;
        e.key = (int)(((float)rand() / RAND_MAX) * 1000);
        e.data = 'T';
        t->deleteElement(e);
    }
    t->forceBufferEmptying();
#endif
/*
    std::cout << "----------- Next try -----------" << std::endl;
    for(int i = 0; i < 100; i++)
    {
        Element e;
        e.key = i;
        e.data = 'T';
        t->deleteElement(e);
    }
*/
    /*
    Element e;
    e.key = 9;
    strcpy(e.data, "Hello2");
    t->deleteElement(e);
    t->findElement(e);
    t->insertElement(e);
    t->findElement(e);
    */

    return 0;
}

int main(int argc, char** argv)
{
    std::ostream* osPtr = &std::cout;
    std::ofstream* file = NULL;
    std::ifstream* inFile = NULL;

    bool printHeader = true;
    /*
    c -- leaf capacity
    b -- minimum node fanout
    B -- maximum node fanout
    l -- buffer length
    n -- number of operations
    f -- node block factor (how many blocks a node)
    M -- maximum amount of memory
    s -- seed for the random number generator
    */

    char* options = "c:C:b:B:l:f:M:i:o:t:T:e";
    static struct option long_options[] = {
        {"min-leaf-capacity",  1, 0, 'c'},
        {"max-leaf-capacity",  1, 0, 'C'},
        {"min-fanout",         1, 0, 'b'},
        {"max-fanout",         1, 0, 'B'},
        {"buffer-length",      1, 0, 'l'},
        {"block-factor",       1, 0, 'f'},
        {"max-memory",         1, 0, 'M'},
        {"input",              1, 0, 'i'},
        {"output",             1, 0, 'o'},
        {"test-type",          1, 0, 't'},
        {"trials",             1, 0, 'T'},
        {"enable-cache",       0, 0, 'e'},
        {0,                    0, 0, 0}
    };

    // if there is no output file specified, output is written to
    // stdout
    int option_index = 0;
    char c;
    // the buffer tree configuration parameter
    TestParameters params;
    int trials = 1;
    while((c = getopt_long(argc, argv, options,
                            long_options, &option_index)) != -1)
    {
        switch(c)
        {
            case 'c':
                params.minLeafCapacity = (size_t)atoi(optarg);
                break;
            case 'C':
                params.maxLeafCapacity = (size_t)atoi(optarg);
                break;
            case 'b':
                params.minNodeFanout = (size_t)atoi(optarg);
                break;
            case 'B':
                params.maxNodeFanout = (size_t)atoi(optarg);
                break;
            case 'l':
                params.bufferLength = (size_t)atoi(optarg);
                break;
            case 'f':
                params.blockFactor = (size_t)atoi(optarg);
                break;
            case 'M':
                params.maxMemory = (size_t)atoi(optarg);
                break;
            case 'i':
                inFile = new std::ifstream(optarg, std::ifstream::binary);
                break;
            case 'o':
                file =
                    new std::ofstream(optarg,
                        std::ofstream::out | std::ofstream::trunc);
                break;
            case 'T':
                trials = (int)atoi(optarg);
                break;
            case 'e':
                params.enableCache = true;
                break;
            case 't':
                for(int i = 0; i < sizeof(typeNames); i++)
                    if(strcmp(optarg, typeNames[i]) == 0)
                    {
                        params.type = (TestType)i;
                        i = sizeof(typeNames);
                    }
                break;
            default:
                std::cerr << "unknown option: " << c << std::endl;
        }
    }
    // check if we got an output file
    if(file != NULL)
        osPtr = file;

    std::ostream& os = *osPtr;

    // check parameters
    if(inFile == NULL)
    {
        std::cerr << "Input file missing." << std::endl;
        return -1;
    }
    if(params.minLeafCapacity < 0
        || params.minLeafCapacity > params.maxLeafCapacity)
    {
        std::cerr << "Invalid min/max leaf capacities: "
            << params.minLeafCapacity << "/" << params.maxLeafCapacity
            << std::endl;
        params.minLeafCapacity = TestParameters().minLeafCapacity;
        params.maxLeafCapacity = TestParameters().maxLeafCapacity;
        std::cerr << "Reverting to defaults: "
            << params.minLeafCapacity << "/" << params.maxLeafCapacity
            << std::endl;
        return -1;
    }
    if(params.minNodeFanout < 0
        || params.minNodeFanout > params.maxNodeFanout)
    {
        std::cerr << "Invalid min/max node fanout: "
            << params.minNodeFanout << "/" << params.maxNodeFanout
            << std::endl;
        params.minNodeFanout = TestParameters().minNodeFanout;
        params.maxNodeFanout = TestParameters().maxNodeFanout;
        std::cerr << "Reverting to defaults: "
            << params.minNodeFanout << "/" << params.maxNodeFanout
            << std::endl;
        return -1;
    }

    // read statistics from input file
    long numOp;
    int opInsert;
    int opFind;
    int opDelete;

    inFile->read((char*)&numOp, sizeof(long));
    inFile->read((char*)&opInsert, sizeof(int));
    inFile->read((char*)&opFind, sizeof(int));
    inFile->read((char*)&opDelete, sizeof(int));
    // remember the file position
    long fpos = inFile->tellg();
    double sum = (double)(opInsert + opFind + opDelete);

    os << "Number of operations: " << numOp << std::endl;
    os << "Ratio of inserts: " << (opInsert / sum) << std::endl;
    os << "Ratio of finds: " << (opFind / sum) << std::endl;
    os << "Ratio of deletes: " << (opDelete / sum) << std::endl;

    BenchmarkInfo avrg;
    for(int i = 0; i < trials; i++)
    {
        inFile->seekg(fpos, std::ifstream::beg);
        std::cerr << "Trial: " << (i + 1) <<  std::endl;
        BenchmarkInfo info;
        if(params.type == BUFFERTREE)
            doBufferTreeTest(os, inFile, numOp, params, info);
        else if(params.type == BTREE)
            doBTreeTest(os, inFile, numOp, params, info);
        avrg.nIOs += info.nIOs;
        avrg.wallTime += info.wallTime;
    }
    os << numOp << "\t"
       << ((double)avrg.nIOs / (double)trials) << "\t"
       << (avrg.wallTime / (double)trials) << std::endl;

    if(file)
        delete file;

    delete inFile;

    return 0;
}
