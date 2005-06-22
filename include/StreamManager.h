/* $Id$ */
#ifndef __STREAMMANAGER_H
#define __STREAMMANAGER_H

#include <stdio.h>
#include <string.h>

/** define type StreamID */
class StreamId
{
#define STREAM_ID_NOID 0
    private:

        int id_;

    public:
        StreamId() : id_(STREAM_ID_NOID) {}
        StreamId(unsigned int id) : id_(id) {}
        StreamId(StreamId& sid) { id_ = sid.id_; }

        StreamId& operator++(int)
        {
            if(id_ == STREAM_ID_NOID)
                id_ = 1;
            else
                id_++;
            return *this;
        }

        StreamId& operator++()
        {
            if(id_ == STREAM_ID_NOID)
                id_ = 1;
            else
                id_++;
            return *this;
        }

        bool operator==(const StreamId& r) { return id_ == r.id_; }

        /**
         * This method appends the string representation of this stream ID
         * to the end of the string given in buf if the string will not
         * exceed len. Otherwise it returns -1.
         */
        int toString(char* buf, size_t len) const
        {
            int retval = -1;
            char* strrep = NULL;

#define ID_STRING_LEN 6
            // reserve memory to store the string representation
            strrep = (char*)malloc(sizeof(char) * (ID_STRING_LEN + 1));
            if(strrep != NULL)
            {
                // write the string representation if the memory could be
                // allocated
                if(snprintf(strrep, ID_STRING_LEN + 1, "%.6d", id_)
                        <= ID_STRING_LEN)
                {
                    // concatenate the two strings if they will not exceed
                    // the allowed length
                    if(strlen(buf) + strlen(strrep) < len)
                    {
                        strncat(buf, strrep, ID_STRING_LEN);
                        retval = 0;
                    }
                }
            }
            free(strrep);
            return retval;
        }

        bool isValid() const { return id_ != STREAM_ID_NOID; }
};

/**
 * Implements a singleton class that provides unique instances of
 * StreamIDs.
 */
class StreamManager
{
    static StreamId last;

    private:

    public:
        static void getStreamId(StreamId& id)
        {
            // only change the stream id if the current one is not valid.
            if(!id.isValid())
                id = StreamId(++last);
        }
};
StreamId StreamManager::last;

#endif
