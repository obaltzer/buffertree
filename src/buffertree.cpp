#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <BufferTree.h>

int main(int argc, char *argv[])
{
    BufferTree* bt = new BufferTree("Hello World");
    bt->print();
    delete bt;
    return EXIT_SUCCESS;
}
