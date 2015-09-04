/**
 * @file pb5_buf.h
 * Provides structures and classes to implement I/O buffer objects for 
 * the PakBus protocol.
 */
/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: pb5_buf.h,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:54:46 $
 *   $State: Exp $
 *****************************************************/

#ifndef MYBUF_H
#define MYBUF_H
#include <iostream>
#include <fstream>
#include <deque>
#include "utils.h"
using namespace std;

#define MAX_PACK_SIZE 1112

/** 
 * Packet structure definition.
 * The structure contains pointers beginning and end of a pakbus packet 
 * in the application input buffer. In case the packet is incomplete, 
 * Packet.Complete is set to false.
 */
typedef struct {
    char *begPacket;
    char *endPacket;
    bool  Complete;
} Packet;

/**
 * I/O Buffer Object for handling PakBus communication.
 * This class is derived from the streambuf class in the standard
 * library. It contains the input and output buffer and provides
 * mechanisms to read and write data to the device identified by the
 * devFd__ member.
 */
class pakbuf : public streambuf {

    public :
        pakbuf (int ibufsize, int obufsize);
        ~pakbuf ();
        deque<Packet>* getPacketQueue () { return &packetQueue__; }
        int            readFromDevice() throw (CommException);
        int            writeToDevice() throw (CommException);
        void           writeRaw() throw (CommException);
        /** Function to get the number of bytes in the output buffer.*/
        int            showManyBytesObuf(){ return (pptr()-pbase()); }
        /** Function to access the beginning of the output buffer. */
        const char*    getobeg () { return pbase(); }
        inline void    setFd(int fd) { devFd__ = fd; }
        void           setHexLogDir(const string& dir);

    protected : 
        void       split_sequence_to_packets (char *beg, char *end);
        // inline int byte2int (char c) { return (0x000000ff & (unsigned char)c); };
        void       traceComm(char *bptr, char *eptr, char type);
        void       unquote_pack (Packet& pack);
        int        quote_msg (char* seq, int len);

    private :
        char         *ibuf__;            // Input buffer
        char         *obuf__;            // Output buffer
        int           ibufsize__;        // Input buffer size
        int           obufsize__;        // Output buffer size
        int           devFd__;          // Device file descriptor
        deque<Packet> packetQueue__;     // Packet queue
        ofstream      ioCommLog__;       // Output file stream for writing I/O byte
                                       // streams to log file
        bool          traceCommEnabled__;   // When set, low-level communication
                                       // with the logger is writen to a log
};

// Used for formatting low-level log output
#define MAX_COUNT_PER_LINE 20 
#endif
