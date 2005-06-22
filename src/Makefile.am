bin_PROGRAMS = buffertree
buffertree_SOURCES = buffertree.cpp

# set the include path found by configure
INCLUDES = -I$(top_srcdir)/include $(all_includes)

# the library search path.
buffertree_LDFLAGS = $(all_libraries) 
lib_LIBRARIES =  libbuffertree.a
libbuffertree_a_SOURCES = BufferTree.cpp
buffertree_LDADD = $(top_builddir)/src/libbuffertree.a
