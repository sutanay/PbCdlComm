/** 
 * @file pb5_proto_base.cpp
 * Implements the base layer for communication with a PakBus device.
 */

/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: pb5_proto_base.cpp,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:55:19 $
 *   $State: Exp $
 *****************************************************/

#include <iostream>
#include <sstream>
#include <fstream>
#include <stdio.h>
#include <unistd.h>
#include <string>
#include "log4cpp/Category.hh"
#include "pb5.h"
#include "utils.h"
using namespace std;
using namespace log4cpp;

/**
 * Function to set debug options. This can be use to control logging of
 * error messages occuring at packet level.
 *
 * @param opt_debug: True value turns on the debug option, which is false
 *                   by default.
 */

bool get_debug () 
{ 
    if (Priority::DEBUG == Category::getRoot().getPriority()) {
        return true;
    }
    else {
        return false;
    }
}
    
/**
 * Function to compute the signature of a byte sequence using the 
 * CSI algorithm.
 */
uint2 CalcSig(const void *buf, uint4 len, uint2 seed)
{
  uint2 j, n;
  uint2 ret = seed;
  byte *ptr = (byte *)buf;

  for (n = 0; n < len; n++) {
      j = ret;
      ret = (ret << 1) & (uint2)0x01ff;
      if(ret >= 0x100){
          ret++;
      }
      ret = (((ret + (j >> 8) + ptr[n]) & (uint2)0xff) | (j << 8));
  }
  return ret;
}

/**
 * This function computes the nullifier of a 2-byte signature computed
 * by the CSI algorithm.
 */
uint2 CalcSigNullifier(uint2 sig)
{
    uint2 tmp, signull;
    byte  null0, null1, msb;

    tmp = (uint2)(0x1ff & (sig << 1));
    if (tmp >= 0x100) {
        tmp += 1;
    }
    null1 = (0x00ff & (0x100 - (sig >> 8) - (0x00ff & tmp)));
    msb = (byte)(0x00ff & sig);
    null0 = (0x00ff & (0x100 - ((0xff & msb))));
    signull = 0xffff & ((null1 << 8) + null0);
    return signull;
}

/**
 * This function serializes an unsigned integer into a byte sequence.
 * Note that the Most Significant Byte is written to the beginning
 * of the sequence.
 * @param ptr: Pointer to the target bytestream
 * @param val: The number to serialize
 * @param len: Number of bytes to serialize. This is useful for data 
 *             types smaller than integer.
 */
void PBSerialize (byte *ptr, uint4 val, uint2 len)
{
    uint2 shift;
    int   i;
    len--;
    for (i = 0; i <= len; i++) {
        shift = 8*(len-i);
        ptr[i] = (byte)(val >> shift);
    }
    return;
}

/**
 * This function extracts an integer from a byte array. It assumes 
 * that the MSB is stored in the first byte of the array. The 
 * funtions an returns unsigned integer - therefore, if the caller
 * is expecting anything else, it'll have to cast the return value.
 * 
 * @param ptr: Pointer to the MSB of the number to be deserialized.
 * @param len: Size of the number to be extracted in bytes.
 */
uint4 PBDeserialize (const byte *ptr, uint2 len) 
{
    uint4 val = 0;
    int   i;
    for (i = 0; i < len; i++) {
        val <<= 8;
        val |= ptr[i];
    }
    return val;
}


/////////////////////////////////////////////////////////////////////
//           Implementation of PakBusMsg class                     //
/////////////////////////////////////////////////////////////////////

/**
 * @param pb_addr : Pointer to the structure containing source address 
 * @param IOBuf : Pointer to the I/O buffer object 
 */
PakBusMsg :: PakBusMsg () : pbuf__(NULL), packetQueue__(NULL), odevs__(NULL)
{
    LinkState__   = 0x0a;
    ExpMoreCode__ = 0x01;
    Priority__    = 0x01;
    SrcPhyAddr__  = 0x0ffe;
    SrcNodeId__   = 0x0ffe;
    HopCnt__      = 0x00;
    TranNbr__     = 0x00;
}

void PakBusMsg :: setPakBusAddr(const PBAddr&  pakbusAddr)
{
    DstPhyAddr__  = pakbusAddr.PakBusID;
    DstNodeId__   = pakbusAddr.NodePakBusID;
    SecurityCode__ = pakbusAddr.SecurityCode;
}

void PakBusMsg :: setIOBuf(pakbuf* IOBuf)
{
    // Set the pointers to the I/O buffer and the queue structure
    // for holding data packets

    odevs__.rdbuf(IOBuf);
    pbuf__ = IOBuf;
    packetQueue__ = pbuf__->getPacketQueue();
    return;
}

/**
 * Function for sending a PakBus packet to the data logger. This 
 * function sort of builds the pakbus header section, calculates the
 * signature of the combined header and message body and sends the 
 * message to the device using the "odevs__" output stream.
 */
void PakBusMsg :: SendPBPacket() throw (CommException)
{
    unsigned short sig, signull;
    byte           nullifier[4];

    if (MsgBodyLen__ < 6 && MsgBodyLen__ > 1000) {
        stringstream msgstrm;
        msgstrm << "Length of message body isn't within the 0-128 range" << endl
                << "Error in sending PakBus message to device : "
                << "MsgType " << hex << byte2int(MsgType__);
        Category::getInstance("PakBusMsg")
                .debug(msgstrm.str());
        return;
    }

    // First, the framing character goes
    odevs__ << SerSyncByte__;

    // Serialize the PakBus header to the o/p buffer
    SerializeHdr();
    odevs__.write((const char *)MsgBody__, MsgBodyLen__);

    // Calculate the signature and signature nullifier. Append the 
    // signature nullifier and framing character to complete the packet
    sig = CalcSig ((void*)(pbuf__->getobeg()+1), pbuf__->showManyBytesObuf()
            - 1, Seed);
    signull = CalcSigNullifier (sig);
    nullifier[0] = (byte)(signull >> 8);
    nullifier[1] = (byte)(signull);

    odevs__.write ((const char *)nullifier, 2);
    odevs__ << SerSyncByte__;
    // Now send the packet down the wire
    pbuf__->writeToDevice();
    return;
}

/**
 * Function for serializing the header section of a PakBus packet
 * into the output stream buffer.
 */
void PakBusMsg :: SerializeHdr ()
{
    Hdr__[0]  = (LinkState__ << 4) | (DstPhyAddr__ >> 8);
    Hdr__[1]  = (byte)(DstPhyAddr__ & 0xff);
    Hdr__[2]  = (ExpMoreCode__ << 6) | (Priority__ << 4) | (SrcPhyAddr__ >> 8);
    Hdr__[3]  = (byte)(SrcPhyAddr__ & 0xff);
    Hdr__[4]  = (HiProtoCode__ << 4) | (DstNodeId__ >> 8);
    Hdr__[5]  = (byte)(DstNodeId__ & 0xff);
    Hdr__[6]  = (HopCnt__ << 4) | (SrcNodeId__ >> 8);
    Hdr__[7]  = (byte)(SrcNodeId__ & 0xff);
    Hdr__[8]  = (byte)MsgType__;
    Hdr__[9] = (byte)TranNbr__;

    odevs__.write((const char *)Hdr__, 10);
    return;
}

/**
 * Function for incrementing the transaction number before sending
 * out a PakBus message. The trasaction number may not be incremented
 * when the same message is repeatedly transmitted to the logger as
 * part of a single task. For example, tasks as fetching the table
 * definition file from the logger may require multiple transmission
 * of the same command to the logger.
 */
byte PakBusMsg :: GenTranNbr()
{
    return ++TranNbr__;
}

void PakBusMsg :: SetSecurityCodeInMsgBody()
{
    MsgBody__[0] = (byte)(SecurityCode__ >> 8);
    MsgBody__[1] = (byte)(SecurityCode__);    
}

/**
 * This function is used to parse a full-fledged PakBus protocol 
 * packet (larger than an average link-state sub-protocol packet). 
 * It carries out basic validations as checking the packet size,
 * signature and destination.
 *
 * The function accepts a message type and a trasaction id as 
 * argument. Typically this function would be called while waiting
 * for a reply from the logger in response to a command. The 
 * arguments correspond to values expected in the received packet.
 *
 * The header section is parsed by the parse_pakbus_header() 
 * function. It also returns a PktSummary structure to the calling
 * ParsePakBusPacket() function. This is useful in determining if
 * any packet other than the desired type was received and take
 * apprpriate action.
 */
int PakBusMsg :: ParsePakBusPacket (Packet& Pack, byte msg_type, byte tran_id) 
        throw (AppException)
{
    PktSummary  digest;
    byte        link_state;
    int         len;
    int         stat;
    // static byte last_msg_type;
    // tatic byte last_tran_id;
    // static int  last_err;
    // static int  successive_err = 0;

    if (!Pack.Complete) {
	return INCOMPLETE_PKT;
    } 
    len = Pack.endPacket - Pack.begPacket + 1;
    if ( (len < 8) || (len > MAX_PACK_SIZE) ) {
        return INVALID_PACKET_SIZE;
    }
    uint2 sig = CalcSig(Pack.begPacket + 1, (uint4)(len-2), 0xaaaa);
    if (sig) {
        return CORRUPT_DATA;
        /*
        if ( (last_err == CORRUPT_DATA) && (last_msg_type == msg_type)
            && (last_tran_id == tran_id) ) {
            successive_err++;
        }
        if (successive_err == 2) {
            throw AppException(__FILE__, __LINE__, "Too many signature errors");
        }
        
        last_msg_type = msg_type;
        last_tran_id  = tran_id;
        last_err      = CORRUPT_DATA;
        return CORRUPT_DATA;
        */
    }
    /*
    else {
        successive_err = 0;
    }
    */

    if ((stat = parse_pakbus_header (Pack, digest))) {
        return stat;
    }

    if (len == 8) {
	link_state = get_link_state (Pack);
        if (link_state == 0x90) {
            // Logger sent a "Ring" msg, respond with "Ready"
            send_link_state_pkt (SERPKT_READY, 4);
        }
        return LINK_STATE_PKT;
    }
     
    if ((digest.TranNbr != tran_id) || (digest.MsgType != msg_type)) {
        if ( !digest.Protocol && (digest.MsgType == 0x09) ) {
            reply_to_hello (digest, Pack);
            return HELLO_MSG;
        }
        else if ( !digest.Protocol && (digest.MsgType == 0x81) ) {
            return DELIVERY_FAILURE;
        }
        else {
            return IGNORE_MSG;
        }
    }
    return SUCCESS;
}

/**
 * Function to parse the PakBus header and obtain summary information.
 * This function performs some error handling by extracting the address
 * information contained in the header section and compare that of the
 * host and the PakBus device connected to it.
 * 
 * @param pack: Reference to the packet structure being parsed.
 * @param Digest: Structure storing the summary of the packet header.
 * @return Returns SUCCESS for a proper PakBus packet.\n
 *         Returns FAILURE if the packet was destined for a different 
 *         device or was not received from a device known to the host,
 *         or if the protocol information in the packet is invalid.
 */
int PakBusMsg :: parse_pakbus_header (Packet& pack, PktSummary& Digest)
{

    int len = pack.endPacket - pack.begPacket + 1;
    byte *ppkt = (byte *)pack.begPacket + 1;

    uint2 DstPhyAddrFrmPkt  = (((0x0f & *ppkt) << 8) | (0x00ff & *(ppkt+1)));
    if (DstPhyAddrFrmPkt != SrcPhyAddr__) {
        return DST_DIFF;
    }
    uint2 SrcPhyAddrFrmPkt  = (((0x0f & *(ppkt+2)) << 8) | (0x00ff & *(ppkt+3)));
    if (SrcPhyAddrFrmPkt != DstPhyAddr__) {
        return SRC_UNKNOWN;
    }
    Digest.SrcPhyAddrFrmPkt = SrcPhyAddrFrmPkt;
    
    if (len == 8) {
        return SUCCESS;
    }

    uint2 DstNodeAddrFrmPkt = (((0x0f & *(ppkt+4)) << 8) | (0x00ff & *(ppkt+5)));
    if (DstNodeAddrFrmPkt != SrcNodeId__) {
        return DST_DIFF;
    }
    uint2 SrcNodeAddrFrmPkt = (((0x0f & *(ppkt+6)) << 8) | (0x00ff & *(ppkt+7)));
    if (SrcNodeAddrFrmPkt != DstNodeId__) {
        return SRC_UNKNOWN;
    }
    Digest.SrcNodeAddrFrmPkt = SrcNodeAddrFrmPkt;
    Digest.Protocol = (byte)((0xf0 & *(ppkt+4)) >> 4);

    if ( (Digest.Protocol != 0) && (Digest.Protocol != 1) ) {
        return INVALID_PROTOCOL;
    }

    Digest.MsgType  = *(ppkt + 8);
    Digest.TranNbr  = *(ppkt + 9);
    return SUCCESS;
}

/**
 * Function to reply to a "Hello" message received from the data
 * logger. The "hello" message would arrive randomly in between the
 * packets being received in response to a particular command.
 * The logger would keep sending the "Hello" message until the 
 * application responds.
 *
 * @param digest: Reference to the PktSummary structure containing 
 *                PakBus address information.
 * @param pack:   Packet that triggers the hello response. 
 */
void PakBusMsg :: reply_to_hello (PktSummary& digest, Packet& pack)
{
    byte  tmp_MsgType;
    byte  tmp_TranNbr;
    byte  tmp_HiProtoCode;
    uint2 tmp_DstPhyAddr;
    uint2 tmp_DstNodeId;
    int   tmp_MsgBodyLen;
    byte  tmp_MsgBody[4];
    int   count;

    // It is important to save the contents of the PakBus Header
    // Because the response to the "Hello" message is being sent
    // in the middle of a transaction

    tmp_HiProtoCode = HiProtoCode__;
    HiProtoCode__   = 0x00;
    tmp_MsgType     = MsgType__;
    MsgType__         = 0x89;         

    tmp_TranNbr     = TranNbr__;
    TranNbr__         = digest.TranNbr;
    tmp_DstPhyAddr  = DstPhyAddr__;
    DstPhyAddr__      = digest.SrcPhyAddrFrmPkt;
    tmp_DstNodeId   = DstNodeId__;
    DstNodeId__       = digest.SrcNodeAddrFrmPkt;
    tmp_MsgBodyLen  = MsgBodyLen__;
    MsgBodyLen__    = 4;

    for (count = 0; count < 4; count++) {
        tmp_MsgBody[count] = MsgBody__[count];
    }

    MsgBody__[0]  = 0x00;                   // Source is not router
    MsgBody__[1]  = *(pack.begPacket + 12); // HopMetric
    MsgBody__[2]  = 0x00; //*(pack.begPacket + 13); // Link verification interval
    MsgBody__[3]  = 0x60; //*(pack.begPacket + 14); // LSB of link verification
                                          // interval
    try {
        SendPBPacket();
    }
    catch (CommException& ce) {
        Category::getInstance("PakBusMsg")
                 .error("Communication error during Hello Transaction");
        throw;
    }

    // Restore original settings
    HiProtoCode__ = tmp_HiProtoCode;
    MsgType__     = tmp_MsgType;
    TranNbr__     = tmp_TranNbr;
    DstPhyAddr__  = tmp_DstPhyAddr;
    DstNodeId__   = tmp_DstNodeId;
    MsgBodyLen__  = tmp_MsgBodyLen;

    for (count = 0; count < 4; count++) {
        MsgBody__[count] = tmp_MsgBody[count];
    }

    return;
}

/**
 * Function to print out diagnostic information when an error
 * is encountered while processing the packet (passed as argument).
 *
 * @param pack: Reference to packet structure that triggered an error.
 * @param stat: Error code returned by ParsePakBusPacket()
 */
void PakBusMsg :: PacketErr (const char *tran_name, Packet& pack, int stat) 
{
    get_debug();
    /*if (!get_debug()) {
        return;
    } */
    // This function prints out descriptive messages with error codes 
    // between values 8-15. They are described in pakbus.h

    if (!(stat & 0x08)) {
        return;
    }

    stringstream msgstrm;
    msgstrm << "Packet Processing error (" << tran_name << ") : "; 

    if (stat == DELIVERY_FAILURE) {
        byte err_code = *(pack.begPacket + 11);

        if (err_code == 0x01) {
            msgstrm << "Delivery failed (Destination unreachable)";
        }
        else if (err_code == 0x02) {
            msgstrm << "Delivery failed (Unreachable higher level protocol)";
        }
        else if (err_code == 0x03) {
            msgstrm << "Delivery failed (Queue overflow)";
        }
        else if (err_code == 0x04) {
            msgstrm << "Delivery failed (Unimplemented command or MsgType)";
        }
        else if (err_code == 0x05) {
            msgstrm << "Delivery failed (Malformed message)";
        }
        else if (err_code == 0x06) {
            msgstrm << "Delivery failed (Link failed)";
        }
        else {
            msgstrm << "Delivery failed (Unknown error)";
        }
    }
    else {
        switch (stat)
        {
            case INVALID_PACKET_SIZE : 
                        msgstrm << "Invalid packet size";
                        break;
            case CORRUPT_DATA :     
                        msgstrm << "Signature test for packet failed";
                        break;
            case IGNORE_MSG :       
                        msgstrm << "Invalid msg type or transaction id";
                        break;
            case DST_DIFF :         
                        msgstrm << "Packet destination different";
                        break;
            case SRC_UNKNOWN :      
                        msgstrm << "Packet source unknown";
                        break;
            case INVALID_PROTOCOL : 
                        msgstrm << "Invalid protocol";
                        break;
            case INCOMPLETE_PKT :   
                        msgstrm << "Incomplete packet";
                        break;
            default :   msgstrm << "Unknown error";
        }
    }

    Category::getInstance("PakBusMsg")
            .debug(msgstrm.str()); 
    return;
}

/**
 * This function sends a series of SerSyncByte__s (0xbd) to the data
 * logger to wake it up.
 *
 */
void PakBusMsg :: InitComm() throw (CommException)
{
    for (int i = 0; i < 12; i++){
        odevs__ << SerSyncByte__;
    }
    pbuf__->writeRaw(); 
    return;
}

/**
 *
 * This function is used to sent a LinkState packet to the data 
 * logger to communicate the status of the link. Currently the 
 * following messages are supported : Ring, Ready, Finished and
 * Broadcast. 
 */
void PakBusMsg :: send_link_state_pkt (int SerPktMsgFormat, int PackSize) 
         throw (CommException)
{
    byte  Msg[10];
    byte  ExpCode = 0x80;
    byte  Prio    = 0x00;
    uint2 sig;
    uint2 signull = 0;
    byte  LinkState;
    uint2 DstAddr = DstPhyAddr__;
    static string LinkMsgType;
    
    switch (SerPktMsgFormat) {
        case SERPKT_RING : 
            LinkState = 0x90;
            LinkMsgType = "Ring";
            break;    
        case SERPKT_READY : 
            LinkState = 0xa0;
            ExpCode   = 0x00;
            LinkMsgType = "Ready";
            break;    
        case SERPKT_FINISHED : 
            LinkState = 0xb0;
            ExpCode   = 0x00;
            LinkMsgType = "Finished";
            break;    
        case SERPKT_BROADCAST : 
            LinkState = 0x80;
            DstAddr   = 0x0fff;
            LinkMsgType = "Broadcast";
            break;    
        default :
            // Write to log?
            return;
    }
    
    Msg[0] = (byte)( LinkState | (DstAddr >> 8) );
    Msg[1] = (byte)( DstAddr & 0xff );
    Msg[2] = (byte)( ExpCode | Prio | (SrcPhyAddr__ >> 8) );
    Msg[3] = (byte)( SrcPhyAddr__ & 0xff );

    odevs__ << SerSyncByte__;

    if (PackSize == 8) {
        Msg[4] = (byte)(DstAddr >> 8);
        Msg[5] = (byte)(DstAddr & 0xff);
        Msg[6] = (byte)(SrcPhyAddr__ >> 8);
        Msg[7] = (byte)(SrcPhyAddr__ & 0xff);
        Msg[8] = (byte)(signull >> 8);
        Msg[9] = (byte)(signull & 0xff);
        // uint2 sig = CalcSig(Msg, 8, Seed);
        // uint2 signull = CalcSigNullifier(sig);
        odevs__.write((const char*)Msg, 10);
    }
    else {
        sig = CalcSig(Msg, 4, Seed);
        signull = CalcSigNullifier(sig);
        Msg[4] = (byte)(signull >> 8);
        Msg[5] = (byte)(signull & 0xff);
        odevs__.write((const char*)Msg, 6);
    }

    odevs__ << SerSyncByte__;
    pbuf__->writeToDevice();
    return;
}

/**
 *  This function is used to carry out a "handshake" process with 
 *  the logger using the SerPkt Link-State Sub Protocol. 
 *  Currently two modes are supported :
 *  1.   If the application sent a "Ring" message and the logger 
 *  responds with "Ready", SUCCESS is returned.
 *  2.   Similarly a "Offline" reply in response to a "Finished" 
 *  message is considered successful. 
 *
 *  If the data logger sends a "Ring" message to the application 
 *  during the handshake process a "Ready" message is sent with 
 *  appropriate format (packets with 8 or 12 byte size).
 *
 */
void PakBusMsg :: HandShake (int mode) throw (CommException, PakBusException)
{
    int  stat;
    int  len;
    byte link_state;
    bool IsOK = false;
    string desc = (mode == SERPKT_RING) ? "RING state" : "FINISHED state";

    send_link_state_pkt (mode, 4);

    try {
        pbuf__->readFromDevice();
    }
    catch (CommException& ce) {
        Category::getInstance("PakBusMsg")
                 .error("No response from device during HandShake");
        throw;
    }

    while ( packetQueue__->size() ) {
        Packet pack = packetQueue__->front();
        stat = ParsePakBusPacket (pack, 0, 0);
        if (stat == LINK_STATE_PKT) {
            link_state = get_link_state (pack);
            if ( (mode == SERPKT_RING) && (link_state == 0xa0) ) {
                IsOK = true;
            }
            // The datalogger returns 0xa0 in response to SERPKT_FINISHED
            // So I'm disabling the check for 0x80
            // else if ( (mode == SERPKT_FINISHED) && (link_state == 0x80) ) {
            else if (mode == SERPKT_FINISHED) {
                IsOK = true;
            }
            else if (link_state == 0x90) {

                len = pack.endPacket - pack.begPacket + 1;
                if (len == 8) {
                    send_link_state_pkt (SERPKT_READY, 4);
                }
                else if (len == 12) {
                    send_link_state_pkt (SERPKT_READY, 8);
                }
            }
        }
        packetQueue__->pop_front ();
    }

    if (false == IsOK) { 
        string errMsg("Handshake failed in ");
        errMsg += desc;
        Category::getInstance("PakBusMsg")
                 .debug(errMsg);
        throw PakBusException(__FILE__, __LINE__,
                errMsg.c_str());
    }    
    else  {
        Category::getInstance("PakBusMsg")
                 .debug("Handshake succeeded for : " + desc);
    }
}


/**
 * Function to carry out basic validations for a Link-State packet.
 * Tests include checking packet size, signature test and checking
 * the packet's destination. 
 * The link state and message length is returned through a structure
 * for replying to the logger (if necessary).
 */
byte PakBusMsg :: get_link_state (Packet& Pack)
{
    byte *ppkt = (byte *)Pack.begPacket + 1;
    byte link_state = (*ppkt & 0xf0);
    return link_state;
}

/*
 The following functions are required for troubleshooting only. They aren't
 required by the applications running on production.
  
unsigned char str2hex (char *ptr)
{
    int msb, lsb;
    if (*ptr < 58) {
        msb = *ptr - 48;
    }
    else {
        msb = toupper (*(ptr)) - 55;
    }

    if (*(ptr+1) < 58) {
        lsb = *(ptr+1) - 48;
    }
    else {
        lsb = toupper (*(ptr+1)) - 55;
    }
    unsigned char byte_num = (msb << 4) + lsb;
    return byte_num;
}

void PakBusMsg :: Simulate (char *file)
{
    char buf[1024];
    char data[256];
    ifstream ifs;
    int nbytes = 0;

    ifs.open (file, ifstream::in);
    ifs.getline (buf, 1024);
    char *ptr = buf;

    while ( (*ptr != '\0') ) {
        data[nbytes++] = (int) str2hex (ptr);
        printf (" %02x ", (int)str2hex(ptr));
        ptr += 2;
        while ( *ptr == ' ' ) ptr++;
    }
    printf ("Number of bytes in loggernet msg : %d\n", nbytes);
    odevs__.write (data, nbytes);
    pbuf__->WriteToDevice();
    sleep (1);
    pbuf__->ReadFromDevice ();
    return;
} */
