#include <iostream>
#include <fstream>
#include <set>
#include <vector>
#include <getopt.h>

#define DEBUG

std::set<long> known_set;
std::vector<long> known_vec;
int size = 0;

long get_unique()
{
    long r;

    do
    {
        r = 1 + (long)(1000000000.0 * rand()/(RAND_MAX + 1.0));
    } while(known_set.find(r) != known_set.end());
    known_set.insert(r);
    known_vec.push_back(r);
    size++;

    return r;
}

long get_id(long pos)
{
//    printf("Get ID: %d\n", pos);
    std::vector<long>::iterator it = known_vec.begin();
    return *(it + pos);
}

long remove_id(long pos)
{
//    printf("Remove ID: %d\n", pos);
    std::vector<long>::iterator it = known_vec.begin();
    it += pos;
    long id = *it;
    known_set.erase(id);
    known_vec.erase(it);
    size--;
    return id;
}

unsigned int my_rand(unsigned int in)
{
    return (1 + (unsigned int)((double)in * rand()/(RAND_MAX + 1.0)));
}

int main(int argc, char** argv)
{
    char* options = "i:d:f:n:o:I";
    static struct option long_options[] = {
        {"insert",  1, 0, 'i'},
        {"delete",  1, 0, 'd'},
        {"find",    1, 0, 'f'},
        {"number",  1, 0, 'n'},
        {"output",  1, 0, 'o'},
        {"insert-first", 0, 0, 'I'},
        {0,         0, 0, 0}
    };

    int option_index = 0;
    char c;

    int opInsert = 1;
    int opFind = 1;
    int opDelete = 1;
    long numOp = 1000000;
    bool insertFirst = false;
    std::ofstream* file = NULL;

    while((c = getopt_long(argc, argv, options,
                            long_options, &option_index)) != -1)
    {
        switch(c)
        {
            case 'i':
                opInsert = (size_t)atoi(optarg);
                break;
            case 'd':
                opDelete = (size_t)atoi(optarg);
                break;
            case 'f':
                opFind = (size_t)atoi(optarg);
                break;
            case 'o':
                file =
                    new std::ofstream(optarg, std::ofstream::binary
                        | std::ofstream::out | std::ofstream::trunc);
                break;
            case 'n':
                numOp = (int)atoi(optarg);
                break;
            case 'I':
                insertFirst = true;
            default:
                std::cerr << "unknown option: " << c << std::endl;
        }
    }
    if(file == NULL)
    {
        std::cerr << "No output file specified." << std::endl;
        exit(-1);
    }
    double sum = (double)(opInsert + opDelete + opFind);
    std::cout << "Number of operations: " << numOp << std::endl;
    std::cout << "Ratio of Inserts: " << (opInsert/sum) << std::endl;
    std::cout << "Ratio of Deletes: " << (opDelete/sum) << std::endl;
    std::cout << "Ratio of Finds: " << (opFind/sum) << std::endl;

    srand(time(NULL));
    char buf[2048];

    // writing statistics
    file->write((char*)&numOp, sizeof(long));
    file->write((char*)&opInsert, sizeof(int));
    file->write((char*)&opFind, sizeof(int));
    file->write((char*)&opDelete, sizeof(int));

    // number of inserts
    long inum = 0l;

    // if insertFirst, perform all the inserts
    if(insertFirst)
    {
        inum = (int)((double)numOp / (double)(opInsert + opFind + opDelete)
                * opInsert);
        char c = 'i';
        for(long i = 0; i < inum; i++)
        {
            long id = get_unique();

            // write the command
            file->write((char*)&c, sizeof(char));
            // write the key
            file->write((char*)&id, sizeof(long));

#ifdef DEBUG
            std::cout << "i " << id << std::endl;
#endif
        }
    }

    for(long i = 0; i < numOp - inum; i++)
    {
        char c;
        int r;
#ifndef DEBUG
        if(i % 1000 == 0)
            std::cout << "Counter: " << i << " Size: " << size
                      << std::endl;
#endif

        if(insertFirst)
        {

            r = 1 + (int)((double)(opDelete + opFind)
                    * rand()/(RAND_MAX + 1.0));
        }
        else
        {
            r = 1 + (int)((double)(opInsert + opDelete + opFind)
                    * rand()/(RAND_MAX + 1.0));
        }

        // insert operation
        if(!insertFirst && r <= opInsert)
        {
            long id = get_unique();
            c = 'i';
            // write the command
            file->write((char*)&c, sizeof(char));
            // write the key
            file->write((char*)&id, sizeof(long));
#ifdef DEBUG
            std::cout << "i " << id << std::endl;
#endif
        }
        // delete operation
        else if((!insertFirst && r <= opInsert + opDelete)
                || (insertFirst && r <= opDelete))
        {
            c = 'd';
            if(size != 0)
            {
                int t = (int)((double)(size - 1)
                        * rand()/(RAND_MAX + 1.0));
                long id = remove_id(t);
                file->write((char*)&c, sizeof(char));
                // write the key
                file->write((char*)&id, sizeof(long));
#ifdef DEBUG
                std::cout << "d " << id << std::endl;
#endif
            }
            else
            {
#ifdef DEBUG
                std::cout << "Skip d" << std::endl;
#endif
                if(!insertFirst)
                    i--;
            }
        }
        // find operation
        else if((!insertFirst && r <= opInsert + opDelete + opFind)
                || (insertFirst && r <= opDelete + opFind))
        {
            c = 'f';
            if(size != 0)
            {
                int t = (int)((double)(size - 1)
                        * rand()/(RAND_MAX + 1.0));
                long id = get_id(t);

                file->write((char*)&c, sizeof(char));
                // write the key
                file->write((char*)&id, sizeof(long));
#ifdef DEBUG
                std::cout << "f " << id << std::endl;
#endif
            }
            else
            {
#ifdef DEBUG
                std::cout << "Skip f" << std::endl;
#endif
                if(!insertFirst)
                    i--;
            }
        }
    }
    file->close();
    delete file;
    return 0;
}
