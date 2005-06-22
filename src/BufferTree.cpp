/* $Id$
 * 
 * Copyright (C) 2004 by Oliver Baltzer <obaltzer@cs.dal.ca>
 */

#include <BufferTree.h>
#include <string>
#include <iostream>

BufferTree::BufferTree(std::string str)
{
    this->str_ = str;
}

void BufferTree::print()
{
    std::cout << "Text is: " << this->str_ << std::endl;
}

BufferTree::~BufferTree()
{
}


