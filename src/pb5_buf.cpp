/** 
 * @file pb5_buf.cpp
 * Implements packet-level communication with the device.
 */

/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: pb5_buf.cpp,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:55:19 $
 *   $State: Exp $
 *****************************************************/

#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <deque>
#include <algorithm>
#include <iterator>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <termios.h>
#include <time.h>
#include "pb5_proto.h"
#include "pb5_buf.h"
#include "utils.h"
#include "log4cpp/Category.hh"

using namespace std;
using namespace log4cpp;

/**
 * Constructor for the pakbus class
 * @param ibuflen: Length of input buffer (stores responses from device)
 * @param obuflen: Length of output buffer (stores outgoing messages)
 * @param filedesc: File descriptor of the target device
 * @param log_dir: Directory for storing low-level log files
 */
pakbuf :: pakbuf(int ibuflen, int obuflen) : devFd__(-1), traceCommEnabled__(false)
{   
    ibuf__ = new char[ibuflen]; 
    obuf__ = new char[obuflen]; 
    ibufsize__ = ibuflen;
    obufsize__ = obuflen;

    // Setup streambuf pointers
    setp(obuf__, obuf__ + obufsize__);
    setg((char *)ibuf__, (char *)ibuf__, (char *)ibuf__);
    return;
}

void pakbuf :: setHexLogDir(const string& log_dir)
{
    struct tm *ptm;
    time_t     t;
    char       log_file[32];

    if (log_dir.size() == 0) {
        return;
    }

    if (traceCommEnabled__) {
        ioCommLog__.close();
    }

    try {
        time (&t);
        ptm = gmtime (&t);
        strftime (log_file, 32, "ComIO.%Y%m%d_%H%M%S.log", ptm);

        string logFilePath (log_dir);
        logFilePath += "/";
        logFilePath += log_file;

        ioCommLog__.open (logFilePath.c_str(), ofstream::out);
        ioCommLog__ << " ---------------- Low-level I/O Log ---------------" << endl;
        traceCommEnabled__ = true;
    } catch (exception& e) {
        Category::getInstance("PB5_BUF").error("Failed to open log-level log file");
    }
}

pakbuf :: ~pakbuf()
{
    // Free-up the input and output buffer memory
    delete [] ibuf__;
    delete [] obuf__;
    // Close the low-level logs if it is open 
    if (ioCommLog__.is_open()) {
        ioCommLog__.close();
    }
    return;
}

/**
 * Function to write the PakBus messages to the low-level log file.
 * Updated by Dennis Oracheski to  print both hex and ascii versions of the 
 * messages in log.
 *
 * @param beg_ptr: Pointer to the beginning of the message.
 * @param end_ptr: Pointer to the end of the message.
 * @param type:    'T' or 'R' to indicate a transmitted/received message.
 */
void pakbuf :: traceComm(char *beg_ptr, char *end_ptr, char type)
{
    if (!traceCommEnabled__) {
        return;
    }

    char  hex[3*MAX_COUNT_PER_LINE+1];    // hex part of output:   3 chars per byte
    char  ascii[MAX_COUNT_PER_LINE+1];    // ascii part of output: 1 char per byte

    char  ctimestamp[32];
    char  type_temp;

    int   count = 0;
    int   i;
    char *ptr;

      // get the time stamp "[yyyy-mm-dd HH:MM:SS]"
      // make copy of the arg 'type'
      //   will be output on 1st line
      //   and are blanked for 2nd,... lines

    strncpy( ctimestamp,get_timestamp(),32 );  ctimestamp[31] = '\0';
    type_temp = type;

//
// loop over all bytes  beg_ptr thru end_ptr inclusive
//
// example output - 'T' transmit asking for TDF table, datalogger 'R' response -
// with MAX_COUNT_PER_LINE set to 10 bytes per line:
//
//  (type) ([date-time]:)     <-- bytes in hexidecimal ---> <- bytes as chars ->
//  'type' 'ctimestamp[]'     'hex[]'                       'ascii[]'
//
//  T [2008-05-26 22:44:25]:  bd a7 2a 6f fe 17 2a 0f fe 1d ..*o..*...
//                            03 00 00 2e 54 44 46 00 00 00 ....TDF...
//                            00 00 00 03 d9 5d 35 bd       .....]5.
//  R [2008-05-26 22:44:27]:  bd af fe 27 2a 1f fe 07 2a 9d ...'*...*.
//                            03 00 00 00 00 00 01 53 74 61 .......Sta
//                            74 75 73 00 00 00 00 01 0e 00 tus.......
//                                (etc)
//

    for (ptr = beg_ptr; ptr <= end_ptr; ptr++) {

        if ( count == 0 ) {
          memset( hex,  (int)' ',3*MAX_COUNT_PER_LINE );
          hex[3*MAX_COUNT_PER_LINE] = '\0';
          memset( ascii,       0,MAX_COUNT_PER_LINE+1 );
        }

           // hex part of output: convert byte to 2 hex digits (followed by space)
        sprintf( &(hex[3*count]),"%2.2x",(unsigned char)*ptr ); hex[3*count+2] = ' ';
           // ascii part of output
           //   if byte is a printable character (space ' ' 0x20 thru '~' 0x7e), 
           //     output it, otherwise, output a period '.'
        if ( (*ptr >= 0x20) && (*ptr <= 0x7e) ) ascii[count] = *ptr;
                                           else ascii[count] = '.';
        count++;

        if (count == MAX_COUNT_PER_LINE) {
            ioCommLog__ << type_temp << " " << ctimestamp << hex << ascii << endl;
            count = 0;
            // arrays 'hex' & 'ascii' are set on start of next loop;
            //   but need to blank the type & timestamp on continuation lines
            for ( i=0 ; ctimestamp[i] != '\0' ; i++ ) ctimestamp[i] = ' ';
            type_temp = ' ';
        }

    }  // next for(ptr)

    if ( count > 0 ) {
      ioCommLog__ << type_temp << " " << ctimestamp << hex << ascii << endl;
    }
    return;
}        

/**
 * Function to read from the device identified by the file descriptor member.
 * A packet queue is built upon reading from the device based on the delimiters
 * (SerSyncByte) in the byte stream read. After the data is read from the 
 * device, the packet queue is traversed to unquote any special symbols from
 * each packet.
 *
 * @return The total number of bytes read from this call.
 */ 

int pakbuf :: readFromDevice() throw (CommException)
{
    int        nbytes;
    int        nread  = 0;
    static uint4 successive_bad_read = 0;
    static int nbytes_last_read = 1; 

    // Initialize the input buffer and the read pointer

    setg((char *)ibuf__, (char *)ibuf__, (char *)ibuf__);
    char *read_ptr = eback ();
    packetQueue__.clear ();
    
    // Read bytes from the serial port. If there are no bytes to read
    // break out of the while loop.
    
    while(1) {
       if ( (nbytes = read (devFd__, read_ptr, 1024)) ){
           nread += nbytes;
           read_ptr += nbytes;
       }
       else {
           if (nread) read_ptr--;
           break;
       } 
    } 

    split_sequence_to_packets ((char *)ibuf__, read_ptr);
    
    setg((char *)ibuf__, (char *)ibuf__, read_ptr);
    deque<Packet>::iterator pack_queue_itr;
    for (pack_queue_itr = packetQueue__.begin(); pack_queue_itr != packetQueue__.end();
            pack_queue_itr++) {
        unquote_pack (*pack_queue_itr);
    }
    tcflush(devFd__, TCIFLUSH);

    if (!nread && !nbytes_last_read) {
        successive_bad_read++;
        if (successive_bad_read == MAX_SUCCESSIVE_BAD_READ) {
            Category::getInstance("I/O")
                     .debug("No response from device");
            throw CommException (__FILE__, __LINE__, "No response from device");
        }
    }
    else {
        successive_bad_read = 0;
    }
    nbytes_last_read = nread;
    return nread;
}

/**
 * Function to split a sequence of bytes into PakBus packets.
 * Each PakBus packet begins and ends with 0xbd. The packets contain 
 * pointers to the beginning and end of each memory segment. Once found,
 * the packets are loaded in a double ended queue.
 *
 * @param beg: Pointer to the beginning of the byte sequence.
 * @param end: Pointer to the end of the byte sequence.
 */
void pakbuf :: split_sequence_to_packets (char *beg, char *end)
{
    char       *packet_beg;
    char       *packet_end;
    char       *beg_search = beg;
    static char start_sym = SerSyncByte__;
    static char end_sym = SerSyncByte__;
    Packet      tmp_pack;
	
    while (beg_search <= end) {
        packet_beg = find (beg_search, end, start_sym);

        if (*packet_beg == start_sym) {
            // A match is found for the beginning of the packet
            tmp_pack.begPacket = packet_beg;

            if (packet_beg == end) {
                    tmp_pack.endPacket = end;
                    tmp_pack.Complete  = false;
                    packetQueue__.push_back(tmp_pack);
                    break;
            }
            else {
                    beg_search = ++packet_beg;
            }
        }
        else {
            // The char sequence does not contain the starting symbol
            // of a packet. 
            break;
        }

        // Now that the beginning of a packet is found, look for the end
        packet_end = find (beg_search, end, end_sym);
        tmp_pack.endPacket = packet_end;

        // Check if the end_sym 
        tmp_pack.Complete = (*packet_end == end_sym) ? true : false;
        packetQueue__.push_back(tmp_pack);
        beg_search = packet_end+1;
    }
    // End of while loop
    return;
}    

/**
 * This function takes a packet as an argument and checks for the
 * presence of the quote byte 0xbc. The value of the byte following 
 * the quote-byte is replaced with the appropriate value. The 
 * beginning and end pointer members of the packet is updated.
 *
 * @param pack: Reference to the PakBus packet to unquote.
 */
void pakbuf :: unquote_pack (Packet& pack)
{
    uint2 len = pack.endPacket - pack.begPacket;
    uint2 i = 0;
    uint2 j = 0;
    int   num_quotebytes = 0;
    byte *tmpbuf = new byte[len];
    byte *pptr;

    // Write the received messages to the low-level log files before they are
    // unquoted. That will allow the users to see the originally received msg.

    traceComm(pack.begPacket, pack.endPacket, 'R');

    // The bytes between the original beginning and end pointer of
    // the packet is copied to a temporary buffer.

    pptr = (byte *)pack.begPacket + 1;
    memcpy ((char *)tmpbuf, (char *)pptr, len); 

    while (i < len) {
        if (tmpbuf[i] == 0xbc) {
            num_quotebytes++;
            i++;
            if (tmpbuf[i] == 0xdd) {
                *(pptr + j) = 0xbd;
            }
            else if (tmpbuf[i] == 0xdc) {
                *(pptr + j) = 0xbc;
            }
            else {
                *(pptr + j) = tmpbuf[i];
            }
            i++;
            j++;
        }
        else {
            // Just keep copying
            *(pptr + j) = tmpbuf[i++];
            j++;
        }
    }
    
    // Move the end pointer of the packet to reduce the effective
    // packet length by the number of quotebytes read

    pack.endPacket -= num_quotebytes;
    delete [] tmpbuf;
    return;
}

/**
 * Function to send a PakBus packet that does not require to be quoted.
 * For example, while sending the initial SerSyncBytes, they don't
 * require any special handling.  
 */
void pakbuf :: writeRaw() throw (CommException)
{
    traceComm(pbase(), pptr()-1, 'T');

    int nbytes = pptr()-pbase();
    int nwrite = write(devFd__, obuf__, nbytes);

    if ((nbytes > 0) && (nwrite == -1)) {
        Category::getInstance("I/O")
                 .debug(strerror(errno));
        throw CommException(__FILE__, __LINE__,
                strerror(errno));
    }

    setp(obuf__, obuf__+obufsize__);
    return;
}

/**
 * Function to send a PakBus message to a PakBus device.
 * Upon being called the function first checks the message in the output
 * buffer and quotes any 0xbc or 0xbd characters contained in the message
 * prior to sending the message to the device.
 */

int pakbuf :: writeToDevice() throw (CommException)
{
    int nbytes; 
    int nwrite; 
    nbytes = quote_msg (pbase(), pptr()-pbase());
    traceComm(pbase(), pbase()+nbytes-1, 'T');
    nwrite = write(devFd__, obuf__, nbytes);

    if ((nbytes > 0) && (nwrite == -1)) {
        Category::getInstance("I/O")
                 .debug(strerror(errno));
        throw CommException(__FILE__, __LINE__,
                strerror(errno));
    }

    setp(obuf__, obuf__ + obufsize__);
    return nwrite;
}

/**
 * Function to check and quote the contents of a PakBus packet.
 * If a PakBus packet contains the 0xbc or 0xbd sysbol, the character 0xbc
 * is inserted in their place followed by 0xdc or 0xdd respectively.
 *
 * @param obuf: Pointer to the beginning of a PakBus packet.
 * @param msg_len: Length of the message to check for quote-bytes.
 */
int pakbuf :: quote_msg (char* seqbuf, int msg_len)
{
    int   tmp_idx = 1; 
    int   count = 1;
    byte* tmpbuf = new byte[msg_len];

    // Copy the input messages to a temporary buffer
    memcpy ((char *)tmpbuf, seqbuf, msg_len);

    while (tmp_idx < (msg_len-1)) {
        if ( (tmpbuf[tmp_idx] == 0xbc) || (tmpbuf[tmp_idx] == 0xbd) ) {
            seqbuf[count] = (char) 0xbc; 
            seqbuf[++count] = tmpbuf[tmp_idx] + 0x20;
        }
        else {
            seqbuf[count] = tmpbuf[tmp_idx];
        }
        count++;
        tmp_idx++;
    }
    seqbuf[count++] = tmpbuf[tmp_idx]; // Get the SerSyncByte at the end of msg
    delete [] tmpbuf;
    return count;
}
