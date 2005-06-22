/* $Id$
 *
 * Copyright (C) 2004 by Oliver Baltzer <ob@baltzer.net>
 */
#ifndef _LOG_H
#define _LOG_H

#include <stdio.h>
#include <assert.h>
#include <stdarg.h>

//#define L_DEBUG log(Logger::LEVEL_DEBUG)
#define L_DEBUG(...)
#define L_INFO log(Logger::LEVEL_INFO)
//#define L_WARN log(Logger::LEVEL_WARN)
#define L_ERROR log(Logger::LEVEL_ERROR)

#define DEBUG 1
#ifdef DEBUG
# define log(l) Logger(__FILE__, __LINE__, l)
#else
# define log(l) EMPTY
# define EMPTY(...)
#endif

class Logger
{
    public:
        enum Level
        {
            LEVEL_DEBUG = 0,
            LEVEL_INFO,
            LEVEL_WARN,
            LEVEL_ERROR
        };

        inline Logger(const char* file, const int line, Level l)
        {
            this->currFile = file;
            this->currLine = line;
            this->currLevel = l;
        }

        inline void operator()(const char* fmt, ...)
        {
            static const char* levels[] =
                {"DEBUG", "INFO", "WARN", "ERROR"};
            //fprintf(stderr, "%s:%d [%s] ", this->currFile, this->currLine,
            //                        levels[this->currLevel]);
            va_list ap;
            va_start(ap, fmt);
            vfprintf(stdout, fmt, ap);
            va_end(ap);
            //fprintf(stderr, "\n");
        }

    private:
        Level currLevel;
        int currLine;
        const char* currFile;
};

#endif
