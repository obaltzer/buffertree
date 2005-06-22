INCLUDES = -I$(top_srcdir)/include -I@top_srcdir@/extern/tpie_latest/include
METASOURCES = AUTO
noinst_PROGRAMS =  bttest
bttest_SOURCES = BufferTreeTest.cc
bttest_LDADD = -L@top_srcdir@/extern/tpie_latest/lib/ -ltpie

AM_CFLAGS = -O3
AM_CXXFLAGS = -O3
bin_PROGRAMS = datagen
datagen_SOURCES = datagen.cc
