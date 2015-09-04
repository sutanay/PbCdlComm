/** 
 * @file pb5_proto.h
 * Defines classes that implement various layers of the PakBus protocol.
 */
/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: pb5_proto.h,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:54:46 $
 *   $State: Exp $
 *****************************************************/

#ifndef PAKBUS5_PROTO_H
#define PAKBUS5_PROTO_H

#include "pb5_buf.h"
#include "pb5_data.h"

const uint2 Seed = 0xaaaa;
const byte  SerSyncByte__ = 0xbd;

const byte  GET_LAST_REC   = 0x05;
const byte  GET_DATA_RANGE = 0x06;
const byte  INQ_REC_INFO   = 0x10;
const byte  STORE_DATA     = 0x20;

uint2 CalcSigNullifier (uint2 sig);
uint2 CalcSig (const void* buf, uint4 len, uint2 seed);

void  PBSerialize (byte* ptr, uint4 val, uint2 len);
uint4 PBDeserialize (const byte* ptr, uint2 len);

unsigned char str2hex (char* ptr);

/**
 * Function to set debug options. This can be use to control logging of
 * error messages occuring at packet level.
 */
 bool get_debug ();

/**
 * Structure used to store the summary information about a PakBus packet.
 * This is used to determine the required action based on its members and
 * perform preliminary error handling.
 */
struct PktSummary {
    PktSummary() : Protocol(0), MsgType(0), TranNbr(0), 
            SrcPhyAddrFrmPkt((uint2)0), SrcNodeAddrFrmPkt((uint2)0) {}
    byte Protocol;
    byte MsgType;
    byte TranNbr;
    uint2 SrcPhyAddrFrmPkt;
    uint2 SrcNodeAddrFrmPkt;
} ;

/**
 * Structure to store to physical address of a PakBus device and its node ID.
 */
typedef struct PBAddr {
    PBAddr() : PakBusID(1), NodePakBusID(1), SecurityCode(0) {}
    int   PakBusID;
    int   NodePakBusID;
    uint2 SecurityCode;
}; 

/**
 * Base class for supporting PakBus communication.
 * This class contains various components of a PakBus message, primarily
 * the header and data section. It also provides the basic functions to
 * establish initial communication with the PakBus device, construct a
 * PakBus message and parse a received data packet to check for errors.
 */
class PakBusMsg {

    public :
        /// Constructor
        PakBusMsg ();
        ~PakBusMsg(){};

        void  setPakBusAddr(const PBAddr& pbAddr);
        void  setIOBuf(pakbuf* IOBuf);
        // Functions for establishing communication
        void  HandShake (int Mode) throw (CommException, PakBusException);
        void  InitComm () throw (CommException);

        // Functions for creating and sending messages
        byte  GenTranNbr ();
        void  SerializeHdr ();
        void  SendPBPacket() throw (CommException);

        // Functions for parsing packets received from the data logger
        int   ParsePakBusPacket (Packet& Pack, byte msg_type, 
                    byte tran_id) throw (AppException); 

        // Functions for displaying error messages from PakCtrl/BMP5 layer
        void  PacketErr (const char *transac_name, Packet& pack, int stat);
        // This function is extremely useful for troubleshooting 
        void  Simulate (char *file);
        void  SetSecurityCodeInMsgBody();

    protected :
        void  send_link_state_pkt (int SerPktMsgFormat, int PackSize) 
                    throw (CommException);
        byte  get_link_state (Packet& Pack);

        int   parse_pakbus_header (Packet& pack, PktSummary& digest);
        void  reply_to_hello (PktSummary& digest, Packet& pack);

        /*
         * Members for creating message body
         */
        byte  Hdr__[20]; 
        byte  MsgBody__[1024];
        int   MsgBodyLen__;
        
        // Members for creating PakBus packet header
        byte  LinkState__;   /**< State of the link : offline, ring, ready, 
                                finished, pause */
        uint2 DstPhyAddr__;  /**< Physical address of the destination node */
        byte  ExpMoreCode__; /**< Field to indicate if the destination node 
                                should expect more packets from source */ 
        uint2 SecurityCode__;
        byte  Priority__;    /**< Range from low (00) to high (03) */
        uint2 SrcPhyAddr__;  /**< Physical address of the source PakBus node */
        byte  HiProtoCode__; /**< Designates the high-level protocol followed
                                in the packet - 0x00: PakCtrl 0x01: BMP5 */
        uint2 DstNodeId__;   /**< Node Id of the message destination */
        byte  HopCnt__;      /**< Always zero when connected directly */
        uint2 SrcNodeId__;   /**< Node id of source */
        byte  MsgType__;     /**< Message type to interpret the information 
                                contained in the packet */
        byte  TranNbr__;     /**< A transaction number is used to identify the
                                the response to a message */

        /** Pointer to the buffer stream class designed for I/O */
        pakbuf* pbuf__;
        /** Packet queue to store packets read from the device */
        deque<Packet>* packetQueue__;

    private :
        /** Output stream attached to the I/O buffer */
        ostream  odevs__;
};

/**
 * This class implements for PakBus Control Protocol for network-level services.
 */
class PakCtrlObj : public PakBusMsg{
    public :
        PakCtrlObj ();
        ~PakCtrlObj() {};
 
        int   HelloTransaction () throw (PakBusException);
        byte  Bye ();

        // The following functions are not required by any application
        // on production. However, they are useful in troubleshooting
        // so I'm leaving them in the source file.
        byte  HelloRequest ();
        void GetSetting (uint2 setting_id);
        int  SetSetting (uint2 setting_id, string val);
        int  SetSetting (uint2 setting_id, uint2 val);
        int  SetSetting (uint2 setting_id, uint4 val);
        // protected functions are used by above functions used 
        // for troubleshooting.
    protected :
        int   generic_set_setting (uint2 setting_id, uint2 setting_len, 
                  byte *val);
        int   devconfig_ctrl_transaction (byte action);
};

struct RecordStat {
     int  count;
     NSec recordTime;
     RecordStat() : count(-1) {}
};

/**
 * This class implements the BMP5 protocol for sending application messages.
 */
#define BMP5_BUFLEN 8192

class BMP5Obj : public PakBusMsg {

    public :
        BMP5Obj ();
        // BMP5Obj (PBAddr* pb_addr, pakbuf* IOBuf, string appl_dir);
	~BMP5Obj ();
        void  setTableDataManager(TableDataManager* tblDataMgr);
        void  getDataDefinitions() throw (IOException, ParseException);
        int   ClockTransaction (uint4 offset_s, uint4 offset_ns);
        int   UploadFile (const char* get_file, char* write_to_file)
                throw (IOException);
        int   DownloadFile (const char *filename);
        int   CollectData (const TableOpt& table_opt) 
                      throw (AppException, invalid_argument);
	int   ControlTable (byte ctrl_opt);
        int   ControlFile (const string& file_name, byte file_cmd);
        int   ReloadTDF ();
 
    protected :
        void  GetProgStats (uint2 security_code) throw (ParseException);
        void  GetTDF () throw (IOException, ParseException);
        int   sendCollectionCmd (byte MessageType, Table& tbl, uint4 P1, uint4 P2);
        RecordStat get_records (Table& tbl_ref, byte mode, int record_size, 
                uint4 P1, uint4 P2, int file_span);
        int   test_data_packet (Table& tbl_ref, Packet& pack) throw (AppException);
        int   store_data (byte* buf, Table& tbl, int beg, int nrecs, int file_span)
                throw (StorageException);
        int   process_upload_file (Packet& pack, ofstream& filedata) 
                throw (IOException);
    
    private :
        byte*     dataBuf__;
        int       dataBufSize__;
        TableDataManager* tblDataMgr__;
};

#define SUCCESS             0
#define FAILURE             1

#define LINK_STATE_PKT      2
#define HELLO_MSG           3

#define SERPKT_RING         4
#define SERPKT_READY        5
#define SERPKT_FINISHED     6
#define SERPKT_BROADCAST    7

// Various codes used to indicate errors in processing packets

#define IGNORE_MSG          8 
#define DST_DIFF            9 
#define SRC_UNKNOWN         10
#define INVALID_PACKET_SIZE 11
#define CORRUPT_DATA        12
#define INVALID_PROTOCOL    13
#define INCOMPLETE_PKT      14
#define DELIVERY_FAILURE    15


// Maximum allowable time offset between host and data logger
#define MAX_TIME_OFFSET     1
#define MAX_SUCCESSIVE_BAD_READ 3
#define MAX_SUCCESSIVE_SIG_ERR  3

#endif
