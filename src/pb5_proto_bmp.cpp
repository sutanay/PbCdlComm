/** 
 * @file pb5_proto_bmp.cpp
 * Contains the BMP5 layer implementation.
 */

/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: pb5_proto_bmp.cpp,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:55:19 $
 *   $State: Exp $
 *****************************************************/

#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <string>
#include <log4cpp/Category.hh>
#include "pb5.h"
#include "utils.h"
using namespace std;
using namespace log4cpp;

/**
 * Implementation of various commands supported by BMP5 protocol  
 * @param: Structure containing the pakbus address of host 
 * @param: I/O Buffer to use for communication
 * @param: Structure containing information about data directory, logger name,
 *         name of tables to collect and the station name.
 */
BMP5Obj :: BMP5Obj () : PakBusMsg(), dataBufSize__(BMP5_BUFLEN), 
        tblDataMgr__(NULL) 
{
    HiProtoCode__ = 0x01;
    dataBuf__ = new byte[dataBufSize__];
}

BMP5Obj :: ~BMP5Obj() 
{
    if (dataBuf__ != NULL) {
        delete [] dataBuf__;
    }
}

void BMP5Obj :: setTableDataManager(TableDataManager* tblDataMgr)
{
    tblDataMgr__ = tblDataMgr;
}

/**
 * Function to check or adjust the time of a PakBus device.
 * A nonzero value for the seconds and nanoseconds arguments will add them 
 * to the clock time. Specify zero to query the time of the PakBus device.
 * 
 * @param secs: Seconds to add to the clock. If the datalogger clock is
 *              ahead of the current time, the adjustment field should be
 *              negative.
 * @param nsecs:Nanoseconds to add to the clock.
 * @return      If the transaction was performed to query the logger time 
 *              (i.e. secs = 0 and nsecs = 0), then the logger time is
 *              returned (measured in number of seconds from 1970). If the
 *              transaction was performed to update the logger clock, 1
 *              is returned on a successful update. Zero is returned in 
 *              either cases to indicate error. 
 */
int 
BMP5Obj :: ClockTransaction (uint4 secs, uint4 nsecs)
{
    int    stat;
    uint4  old_time;
    int    ret_value = 0;

    Priority__ = 0x02;
    MsgType__  = 0x17;
    MsgBodyLen__ = 10;

    SetSecurityCodeInMsgBody();
    // Rest of the message body contains the adjustment to the clock
    // in seconds. It would be zero for a check.
    PBSerialize (MsgBody__ + 2, secs, 4);
    PBSerialize (MsgBody__ + 6, nsecs, 4);
    byte tran_id = GenTranNbr();

    try {
        SendPBPacket();
        pbuf__->readFromDevice();
    }
    catch (CommException& ce) {
        Category::getInstance("BMP5")
                 .error("Communication error during Clock Transaction");
        throw;
    }

    while (packetQueue__->size()) {
        Packet pack = packetQueue__->front();
        stat = ParsePakBusPacket (pack, 0x97, tran_id);

        if (stat) {
            PacketErr ("Clock Transaction", pack, stat);
        }
        else {
            if (!secs && !nsecs) {
                // If the transaction was to query datalogger time
                // return datalogger time
                old_time = PBDeserialize ((byte *)pack.begPacket + 12, 4);
                ret_value = old_time + SECS_BEFORE_1990;
            }
            else {
                // If the transaction was to update datalogger time
                // return response code
                ret_value = *(pack.begPacket + 11) ? 1 : 0;
            }
        }
        packetQueue__->pop_front ();
    }
    return ret_value;
}


/**
 * Function to collect the table definitions file stored on the data logger.
 * First, the function would try to read the <DATA_DIR>/conf/tdf.dat file
 * and build the table definitions from it. If not found, it'll fetch
 * the table definitions file from the logger. Also an XML file called tdf.xml
 * will be created in the <DATA_DIR>/conf directory.
 *
 * @return Throws AppException on failure.
 */
void 
BMP5Obj :: getDataDefinitions() throw (IOException, ParseException)
{
    this->GetTDF();
    this->GetProgStats((uint2)0);

    int maxRecordSize = tblDataMgr__->getMaxRecordSize();
    if (maxRecordSize > dataBufSize__) {
        delete [] dataBuf__;
        dataBufSize__ = maxRecordSize;
        dataBuf__ = new byte[dataBufSize__];
    }
    return;
}

void 
BMP5Obj :: GetTDF () throw (IOException, ParseException)
{
    int    stat;
    string tdf_file = tblDataMgr__->getDataOutputConfig().WorkingPath;
    tdf_file += "/.working/tdf.dat";

    string tdf_file_tmp = tdf_file;
    tdf_file_tmp += ".tmp";

    if ( (stat = tblDataMgr__->BuildTDF()) == FAILURE ) {

        Category::getInstance("BMP5")
                .info("Uploading table definitions file from the logger ...");

        if (UploadFile(".TDF", (char *)tdf_file_tmp.c_str()) == FAILURE) {
            throw ParseException(__FILE__, __LINE__,
                    "TDF parsing failed due to failure in uploading file");
        }

        int renameStatus = rename(tdf_file_tmp.c_str(), tdf_file.c_str());
        if (renameStatus != 0) {
            Category::getInstance("BMP5")
                     .error("Failed to rename temporary file to : " + tdf_file);
            unlink(tdf_file_tmp.c_str());
            throw IOException (__FILE__, __LINE__,
                    "TDF parsing failed due to rename error");
        }

        stat = tblDataMgr__->BuildTDF();
        if (stat == FAILURE) {
            Category::getInstance("BMP5")
                     .info("Failed to parse TDF file following download from logger");
            throw ParseException (__FILE__, __LINE__,
                    "Failed to parse TDF file following download from logger");
        }
    }
    return;
}

int 
BMP5Obj :: ReloadTDF () 
{
    tblDataMgr__->cleanCache();
    Category::getInstance("BMP5")
            .info("Recollecting table definitions file from data logger");

    try {
        this->GetTDF();
    } catch (AppException e) {
        Category::getInstance("BMP5").error(e.what());
        return FAILURE;
    }
    return SUCCESS;
}

/**
 * Function to download a file to the data logger from host.
 * @param filename: Name of file to download to the logger.
 * @return SUCCESS | FAILURE
 */
int 
BMP5Obj :: DownloadFile (const char *filename)
{
    char     buf[400];
    int      buf_size = 400;
    ifstream ifs (filename);
    // Assuming that the file will be downloaded to the CPU:
    // directory on the data logger
    string   store_file("CPU:");
    int      file_offset = 0;
    int      nread = 0;
    byte     close_flag = 0;
    byte     resp_code = 0;
    byte     tran_id;
    uint4    resp_offset = 0;
    int      stat = 0;

    store_file += filename;
    int      len = store_file.size();

    MsgType__ = 0x1c;
    // Including the null-character at the end of the string
    MsgBodyLen__ = buf_size + 9 + len;

    SetSecurityCodeInMsgBody();
    memcpy (MsgBody__+2, store_file.c_str(), len + 1);
    MsgBody__[len+3] = 0x00;
    tran_id = GenTranNbr();

    while (!ifs.eof()) {
        // Checking the stat variable is important because that
        // gets set when the previous command packet wasn't sent
        // successfully. If it is set, the same packet gets sent. 

        if (!stat) {
	    ifs.read (buf, buf_size);
	    nread = ifs.gcount(); 
            // Assuming read() reached end of file
	    if (nread < buf_size) {
	        close_flag = 0x01;
                MsgBodyLen__ = len + 9 + nread;
            }
	    else {
	        close_flag = 0x00;
            }
        }

        MsgBody__[len+4] = close_flag;
        PBSerialize (MsgBody__+len+5, (uint4)file_offset, 4);
        memcpy (MsgBody__+len+9, buf, nread);

        try {
            SendPBPacket();
            pbuf__->readFromDevice();
        } 
        catch (CommException& ce) {
            Category::getInstance("BMP5")
                     .error("Communication error during File Download Transaction");
            throw;
        }

        while (packetQueue__->size()) {
            Packet pack = packetQueue__->front();
            stat = ParsePakBusPacket (pack, 0x9c, tran_id);

            if (stat) {
                PacketErr ("File Download Transaction", pack, stat);
            }
            else {
		        resp_code = (byte)*(pack.begPacket+11);
                resp_offset = PBDeserialize ((byte *)(pack.begPacket+12), 4);
            }
            packetQueue__->pop_front ();
        }

        if (!stat) {
            if (resp_code || (resp_offset != (uint4)file_offset)) {
	        return resp_code;
            }
	    file_offset += nread;
        }
    }
    return SUCCESS;
}

/**
 * Function to upload a file from the data logger to the host. The
 * function requires the name of the file to download and the complete
 * path for the output file for writing out downloaded data.
 * @param get_file: File to upload from logger to host. This should
 *                  include the complete pathname (i.e. CPU:Def.TDF 
 *                  instead of Def.TDF.
 * @param write_to_file: Complete path of the destination filename on 
 *                  host.
 * @return SUCCESS | FAILURE
 */ 
int 
BMP5Obj :: UploadFile (const char *get_file, char *write_to_file) throw (IOException)
{
    int      len, stat = FAILURE;
    uint4    file_offset = 0;
    uint4    file_datalen = 0;
    Packet   pack;
    ofstream TDFdata;
    bool     ioException = false;
    
    // Maximum number of bytes that can be packed in a PakBus message
    // with exceeding 1010 bytes. Actually it would be 1010 -8 (PB Hdr)
    // - 2 (Nullifier) - 7 (bytes from the response MsgBody) = 993.
    // I'm rounding off to 985.
    
    Priority__ = 0x02;
    MsgType__  = 0x1d;
    // uint2 Swath = 0x0158;
    uint2 Swath = 0x03d9;
    byte  tran_id =  GenTranNbr();

    // Keep the file open for possible exchanges, the logger will close
    // the file automatically is complete file is read
    byte  CloseFlag = 0x00;

    SetSecurityCodeInMsgBody();

    len = strlen (get_file);
    memcpy (MsgBody__+2, get_file, len);
    MsgBody__[len+2] = 0x00;

    MsgBody__[len+3] = CloseFlag;

    // Byte offset into the file is set within the loop below

    MsgBody__[len+8] = (byte)(Swath >> 8);
    MsgBody__[len+9] = (byte)(Swath & 0xff);

    MsgBodyLen__ = len+10;

    TDFdata.open (write_to_file, ofstream::binary | ofstream::out);
    
    if (TDFdata.is_open() == false) {
        string err("Failed to open : ");
        err.append(write_to_file);
        Category::getInstance("BMP5")
                 .error(err);
        throw IOException(__FILE__, __LINE__, err.c_str());
    }

    while (1) {
        PBSerialize (MsgBody__+len+4, (uint4)file_offset, 4);

        try {
            SendPBPacket();
            pbuf__->readFromDevice();
        }
        catch (CommException& ce) {
            Category::getInstance("BMP5")
                     .error("Communication error during File Upload Transaction");
            throw;
        }

        if (!packetQueue__->size()) {
            Category::getInstance("BMP5")
                    .warn("No data was found to read.");
            break;
        }

        // There will be zero or one packets containing response
        // to Uploadfile command and some hello packets

        while (packetQueue__->size()) {
            pack = packetQueue__->front();
            stat = ParsePakBusPacket (pack, 0x9d, tran_id);
            if (stat) {
                PacketErr ("File Upload Transaction", pack, stat);
            }
            else {
                try {
                    file_datalen = process_upload_file (pack, TDFdata);
                } 
                catch (IOException& ioe) {
                    string err("I/O error occurred while writing to : ");
                    err.append(write_to_file);
                    Category::getInstance("BMP5").warn(err);
                    ioException = true;
                    break;
                }
                file_offset += file_datalen;
            }
            packetQueue__->pop_front ();
        }

        if (ioException) { 
            break;
        }

        if (!stat) {
            if (file_datalen != Swath) {
                break;
            }
        }
        else {
            if (stat == DELIVERY_FAILURE) {
                break;
            }
            else {
                sleep (1);
            }
        }
    }
    
    TDFdata.close ();

    if (ioException) {
        string err("Removing possibly corrupted file : ");
        err.append(write_to_file);
        Category::getInstance("BMP5").notice(err);
        unlink(write_to_file);
    }
    
    if (ioException || (file_offset == 0) || stat) {
        // Indicate that this transaction is the final exchange of this
        // trasaction so that the file can be closed.
        // CloseFlag = 0x01;

        MsgBody__[len+3] = 0x01;
        MsgBody__[len+8] = 0x00;
        MsgBody__[len+9] = 0x00;

        try {
            SendPBPacket();
            pbuf__->readFromDevice();
        } 
        catch (CommException& ce) {
            Category::getInstance("BMP5")
                     .error("Communication error during closing of File Upload Transaction");
        }
        return FAILURE;
    }
    else {
        return SUCCESS;
    }
}

/**
 * Function to parse the data packets received in response to UpLoadFile command.
 * @param pack: Refernce to the PakBus data packet to parse.
 * @param filedata: Reference to the output filestream to write the data 
 *              in the packet. Assuming the file stream will be opened prior
 *              to making the function call.
 * @return On success, returns the number of bytes from the file received in 
 * the packet. 0 is returned to indicate error conditions.
 */
int 
BMP5Obj :: process_upload_file (Packet& pack, ofstream& filedata) throw (IOException)
{
    byte *pack_ptr = (byte *)(pack.begPacket + 11);
    string errormsg;

    byte stat = *pack_ptr++;
    if (stat) {
        if (stat == 0x01) {
            errormsg = "Permission denied";
        }
        else if (stat == 0x0d) {
            errormsg = "Invalid filename";
        }
        else if (stat == 0x0e) {
            errormsg = "File currently unavailable";
        }
        Category::getInstance("BMP5")
                .error("process_upload_file() : " + errormsg);
        return 0;
    }
    pack_ptr += 4;

    int file_data_len = pack.endPacket - (char *)pack_ptr - 2;
    // Need to subtract the signature length as well
    filedata.write ((const char *)pack_ptr, file_data_len);
    filedata.flush();
    if (filedata.bad()) {
        throw IOException(__FILE__, __LINE__, "I/O error");
    }
    return file_data_len;
}

/**
 * Function to send a "collect" command to the data logger. 
 * Although the BMP5 protocol describes 6 modes for data collection, the 
 * data logger software may not have the features implemented. 
 * @param message_type: Message type to indicate the data collection.
 *              mode. Modes 0x03-0x07 are implenented in this function,
 *              although only 0x05 and 0x06 are used in this program
 *              for data collection.
 * @param tbl:  Reference to the Table structure containing information
 *              about the table to collect data from.
 * @param P1, P2: Parameters corresponding to the data collection mode.
 *              If a collection mode requires only one parameter, then
 *              the second argument (P2) will be ignored.
 * @return Returns -1 if an invalud collection mode is specified.
 */
int 
BMP5Obj :: sendCollectionCmd (byte message_type, Table& tbl, uint4 P1, uint4 P2)
{
    Priority__ = 0x02;
    MsgType__  = 0x09;
    stringstream msgstrm;

    switch (message_type) {
        // Collect records between P1 and P2. Include P1 but exclude P2
        case 0x06 : MsgBodyLen__ = 17;
                    break;
        // Collect last P1 records
        case 0x05 : MsgBodyLen__ = 13;
                    break;
        // Collect a partial record with P1 describing the record number
        // while P2 specifies the byte offset into the record.
        case 0x08 : MsgBodyLen__ = 17;
                    break;
        // Get all the data stored on the logger
        case 0x03 : MsgBodyLen__ = 9;
                    break;
        // Collect from P1 to the latest record
        case 0x04 : MsgBodyLen__ = 13;
                    break;
        // Collect a Time Swath described by P1 and P2
        case 0x07 : MsgBodyLen__ = 17;
                    break;
        // Get all the data stored on the logger
        default   : return -1;
    }

    SetSecurityCodeInMsgBody();
    MsgBody__[2] = message_type;
    PBSerialize (MsgBody__+3, tbl.TblNum, 2);
    PBSerialize (MsgBody__+5, tbl.TblSignature, 2);

    if (MsgBodyLen__ == 9) {
        // No parameters required, just pass the field list terminator
        PBSerialize (MsgBody__+7, 0, 2);
    }
    else {
        PBSerialize (MsgBody__+7, P1, 4);

        if (MsgBodyLen__ == 13) {
            PBSerialize (MsgBody__+11, 0, 2);
        }
        else {
            PBSerialize (MsgBody__+11, P2, 4);
            PBSerialize (MsgBody__+15, 0, 2);
        }
    }
    SendPBPacket();
    return TranNbr__;
}

/**
 * Function to store data for a table from a bytesequence.
 * This function uses the information stored in Table Definition File to 
 * extract data for a specified table from a given bytestream. Typically the 
 * byte sequence points to the data section of a PakBus packet or in case,
 * a large data record is fragmented into multiple packets, it will point to
 * the beginning of a buffer where data is stored.
 *
 * @param buf: Pointer to the data section of a PakBus packet or in case a 
 *             large data record is fragmented in multiple packets, this would
 *             point to the beginning of a buffer where data would be stored. 
 * @param tbl: Reference to the Table structure for which data will be 
 *             extracted from byte sequence and stored.
 * @param beg: Record number of the first data record to extract.
 * @param nrecs: Number of records to extract.
 * @param file_span: Span of a datafile in seconds.
 * @return stat: Returns status to indicate if the data extraction and storage
 *             was successful.
 */
int 
BMP5Obj :: store_data (byte* buf, Table& tbl, int beg, int nrecs,
        int file_span) throw (StorageException)
{
    int stat = FAILURE;
    bool parseTimestamp = true;
    int rec_num = 0;
    while (rec_num < nrecs) {
        try {
            stat = tblDataMgr__->storeRecord (tbl, &buf, beg+rec_num, file_span, 
                    parseTimestamp);
        } catch (StorageException& e) {
            Category::getInstance("BMP5")
                     .error("Caught exception while storing data for " + tbl.TblName);
            Category::getInstance("BMP5")
                     .error(e.what()); 
            throw;
        }
        parseTimestamp = false;
        if (stat == FAILURE) {
            break;
        }
        rec_num++;
    } 
    return stat;
}

/**
 * Function to collect data from a specified table. 
 * First a message is sent to the data logger to query about the last stored
 * record. Next, the record number to begin the collection from is determined.
 * If the record number collected during is last attempt is known, then 
 * the collection begins from the record next to it. Else, the collection
 * begins from the earliest possible record. The collection goes one record
 * at a time untill the response to collect command returns an empty
 * data section. Data packets are parsed using process_data_packet().
 * It calls storeRecord() in turn to actually extract records for the
 * specified table from the byte sequence and write them to disk.
 *
 * @param table_opt: Structure containing table name and span information.
 * @param span: Span of datafile in seconds
 * @return SUCCESS | FAILURE
 */
int 
BMP5Obj :: CollectData (const TableOpt& table_opt) throw (AppException, invalid_argument)
{
    // bool     alloc_buffer = true;
    int      record_size;
    int      last_rec_nbr;
    int      nrecs_read = 0;
    uint4    recs_per_request = 1;
    int      records_pending;
    uint4    num_collected_recs = 0;
    stringstream msgstrm;
    RecordStat recordStat;

    Table& tbl_ref = tblDataMgr__->getTableRef (table_opt.TableName);
    
    record_size = tblDataMgr__->getRecordSize (tbl_ref);

    // If there is a possibility of the data record be fragmented into multiple
    // packets, then allocate buffer memory to accumulate the data stored in 
    // incoming packets.

    /*
    if ( (record_size > 512) ) {
        // dataBuf__ = new byte[record_size];
        recs_per_request = 1;
    }
    else if (record_size == -1) {
        // This is the case when a record may have a variable size member. 
        // Allocate a large memory chunk of arbitrary size.
        // dataBuf__ = new byte[3000];
        recs_per_request = 1;
    }
    else */
    if ((record_size > 0) && (record_size < 512)) {
        // alloc_buffer = false;
        recs_per_request = (uint4) (512/record_size);
    }

    // If the table size is known 

    if (tbl_ref.TblSize > 1) {

        int numAttempts = 0;
        // get the last record number the logger is written in it's memory.
        while (numAttempts < 3) {
            recordStat = get_records (tbl_ref, GET_LAST_REC | INQ_REC_INFO,
                    record_size, 1, 0, table_opt.TableSpan);
            last_rec_nbr = recordStat.count;
            if (last_rec_nbr >= 0) {
                break;
            }
        }

        // The data can't be collected if the last record number is unavailable.
        if (last_rec_nbr < 0) {
            string err("Failed to retrieve information about last record stored in [");
            err.append(tbl_ref.TblName)
                  .append("] on datalogger memory");
            Category::getInstance("BMP5")
                     .error(err);
            return FAILURE;
        }

        // Get the record number to start data collection and the record 
        // untill which to collect. Table::NextRecord tells us where we need to
        // start the data collection.
   
        msgstrm << "Record Index information :" << endl
                << "\t\tIndex of last stored record on datalogger memory : " 
                << last_rec_nbr << endl
                << "\t\tIndex of next record to collect from datalogger memory : " 
                << tbl_ref.NextRecord;
        Category::getInstance("BMP5")
                 .debug(msgstrm.str());
        msgstrm.str("");
         
        records_pending = (int)(last_rec_nbr-tbl_ref.NextRecord);

        if  (records_pending < 0) {

            time_t t_s = (time_t)tbl_ref.LastRecordTime.sec + SECS_BEFORE_1990;
            time_t t_c = (time_t)recordStat.recordTime.sec + SECS_BEFORE_1990;

            if (records_pending == -1) {
                if (0 == nseccmp(tbl_ref.LastRecordTime, recordStat.recordTime)) {
                    Category::getInstance("BMP5")
                         .info("No new data is available yet for : " 
                                + table_opt.TableName);
                    return SUCCESS;
                }
                else {
                    msgstrm << "Different timestamp found for identical record id\n"
                            << "\tTimestamp of last stored record on logger : " 
                            << ctime(&t_s)
                            << "\tTimestamp of last collected record from logger : " 
                            << ctime(&t_c);
                    Category::getInstance("BMP5")
                             .notice(msgstrm.str());
                    msgstrm.str("");
                }
            }
            else if (nseccmp(tbl_ref.LastRecordTime, recordStat.recordTime) > 1) {
                msgstrm << "Backward shift observed in datalogger clock." << endl
                        << "\tCheck data from table => " << tbl_ref.TblName << endl
                        << "\tTimestamp of last available data record in datalogger memory"
                        << "precedes the timestamp of the last collected record" << endl
                        << "\tNext target record index : " << tbl_ref.NextRecord << endl
                        << "\tTimestamp of last collected record from datalogger: " 
                        << ctime(&t_c)
                        << "\tIndex of last stored record in datalogger memory : " 
                        << last_rec_nbr << endl
                        << "\tTimestamp of last stored record in datalogger memory : " 
                        << ctime(&t_s);
                Category::getInstance("BMP5")
                         .warn(msgstrm.str());
                msgstrm.str("");
            }
        } 

        // The following cases need special attention:
        // 1. The logger has written enough data since the last data collection so
        //    that the record with ID tbl_ref.NextRecord is wiped from memory. In 
        //    this case, we need to set the "begin pointer" at the beginning of the
        //    table.
        // 2. The data collection downtime can be long enough so that the logger 
        //    reached the maximum  record id and then started back from 1 again.

        if ((records_pending >= (int)tbl_ref.TblSize) || (records_pending < 0)) {

            msgstrm << "Adjusting start record index to compensate for backlog:\n"
                    << "\tTable(" << tbl_ref.TblName << ") size: "
                    << tbl_ref.TblSize << " records" << endl
                    << "\tLast stored record id : " << last_rec_nbr << endl
                    << "\tLast collected record id : " << tbl_ref.NextRecord << endl;

            int newIndex = last_rec_nbr - tbl_ref.TblSize + 2;
      
            tbl_ref.NextRecord = (newIndex < 0) ? 1 : ((uint4)newIndex);

            msgstrm << "\tAdvancing next collection record to : ";
            msgstrm << tbl_ref.NextRecord << endl;

            Category::getInstance("BMP5").info(msgstrm.str());
            msgstrm.str("");
         
            // Reset all the history for this Table
            if (tbl_ref.NewFileTime) {
                tblDataMgr__->flushTableDataCache(tbl_ref);
            }
        }
    
        // If the temporary data file for this table already exists, 
        // append to it. Else, a new file will be created.
    
        //TODO set the fileSpan/reportSpan here and remove from the get_records call
        tblDataMgr__->getTableDataWriter()->initWrite(tbl_ref);

       /*
        * Main collection loop
        */
 
        uint4 lastBadRecordIndex = (unsigned int) -1;
        int countBadRecordCollAttempt = 0;
        int MAX_BAD_REC_COLL_REATTEMPT = 2;

        while (tbl_ref.NextRecord <= (uint4) last_rec_nbr) 
        {
            recordStat = get_records (tbl_ref, GET_DATA_RANGE | STORE_DATA,
                    record_size, tbl_ref.NextRecord, 
                    tbl_ref.NextRecord + recs_per_request, table_opt.TableSpan);
            nrecs_read = recordStat.count;

            if (nrecs_read < 0) {
                break;
            }
            else if (nrecs_read == 0) {
                if (lastBadRecordIndex != tbl_ref.NextRecord) {
                    countBadRecordCollAttempt += 1;
                    lastBadRecordIndex = tbl_ref.NextRecord;
                }
                else if (countBadRecordCollAttempt < MAX_BAD_REC_COLL_REATTEMPT) {
                    countBadRecordCollAttempt++;
                }
                else {
                    countBadRecordCollAttempt = 0;
                    msgstrm << "Failed to collect record with index " 
                            << tbl_ref.NextRecord << " (" << (MAX_BAD_REC_COLL_REATTEMPT+1)
                            << " attempts failed)";
                    Category::getInstance("BMP5")
                             .error(msgstrm.str());
                    msgstrm.str("");
 
                    tbl_ref.NextRecord += 1;
                    msgstrm << "Advancing collection to record index : "
                            << tbl_ref.NextRecord;
                    Category::getInstance("BMP5")
                             .notice(msgstrm.str());
                    msgstrm.str("");
                } 
            }
            else {
                // tbl_ref.NextRecord += nrecs_read;
                num_collected_recs += nrecs_read;
            }
        }
       
        tblDataMgr__->getTableDataWriter()->finishWrite(tbl_ref);
    }
    else {
        //
        // For the case where TblSize (number of records in a table)
        // is not known
        //
        tblDataMgr__->getTableDataWriter()->initWrite(tbl_ref);

        recordStat = get_records (tbl_ref, GET_LAST_REC | STORE_DATA,
                record_size, 1, 0, table_opt.TableSpan);
        num_collected_recs = recordStat.count;
       
        tblDataMgr__->getTableDataWriter()->finishWrite(tbl_ref);
    }

    if (get_debug()) {
        msgstrm << "Collected " << num_collected_recs << " records from " 
                << tbl_ref.TblName;
        Category::getInstance("BMP5").debug(msgstrm.str());
        msgstrm.str("");
    }

    /* if (alloc_buffer) {
        delete [] dataBuf__;
    }*/

    if ((table_opt.SampleInt >= 0) && (tbl_ref.LastRecordTime.sec > 0)) {
        if ((tbl_ref.LastRecordTime.sec + table_opt.SampleInt) 
                >= tbl_ref.NewFileTime) {
            tblDataMgr__->flushTableDataCache(tbl_ref);
        }
    }
    
    // A negative nrecs_read indicates some sort of error in data collection
    if (nrecs_read >= 0) {
        return SUCCESS;
    }
    else {
        return FAILURE;
    }
}

/**
 * Function for sending a message to administer tables on the datalogger.
 * @param ctrl_opt: 0x01 (Reset the table and trash existing records)\n 
 *                  0x02 (Roll over to a new file if the tables are managed
 *                        in files)
 * @return Returns SUCCESS if the Response code is "Complete". FAILIRE if
 * the response code indicates permission denied, or invalid option/table 
 * name.
 */
int 
BMP5Obj :: ControlTable (byte ctrl_opt)
{
    int  stat;
    byte resp_code = 0;

    Priority__ = 0x02;
    MsgType__  = 0x19;
    MsgBodyLen__ = 3;

    SetSecurityCodeInMsgBody();
    MsgBody__[2] = ctrl_opt; // Control option code

    byte tran_id = GenTranNbr();

    try {
        SendPBPacket();
        pbuf__->readFromDevice();
    } catch (CommException& ce) {
        Category::getInstance("BMP5")
                 .error("Communication error during Control Table transaction");
        throw;
    }

    while (packetQueue__->size()) {
        Packet pack = packetQueue__->front();
        stat = ParsePakBusPacket (pack, 0x99, tran_id);
        if (stat) {
            PacketErr ("Control Table Transaction", pack, stat);
        }
        else {
	    resp_code = (byte)*(pack.begPacket + 11);
        }
        packetQueue__->pop_front ();
    }

    if (resp_code == 0x00) {
	return SUCCESS;
    }
    else {
	return FAILURE;
    }
}

/**
 * Function to manage program and data files on a data logger.
 * The file control transaction controls compilation execution of the data-
 * logger program and manages files on the data logger. Following options are 
 * supported :
 * 0x01: Compile and run the program and also make it "run on power-up" file.\n
 * 0x04: Delete this file.\n
 * 0x05: Format the device.\n
 * 0x06: Compile and run the program without deleting data files\n
 * 0x07: Stop the running program\n
 * 0x08: Stop the running program and delete associated data files.\n
 * 0x0a: Compile and run the program without changing "run on power-up" attribute.\n
 * 0x0c: Resume running program execution.\n
 * 0x0d: Stop the running program, delete associated data files, run specified 
 *       file and set it to "run on power-up".\n
 * 0x0e: Stop the running program, delete associated data files, run specified 
 *       file but don't change the "run on power-up" attribute.
 * 
 * @param file_name: Complete name of the program file (i.e. specify CPU:prog.CR1
 *                   than prog.CR1)
 * @param cmd: Code to specify the command to execute. 
 */
int 
BMP5Obj :: ControlFile (const string& file_name, byte cmd)
{
    int  stat;
    byte resp_code = 0x01;
    int  hold_off  = 0;
    int  len = file_name.size();

    Priority__ = 0x02;
    MsgType__  = 0x1e;
    MsgBodyLen__ = 3 + len + 1;

    SetSecurityCodeInMsgBody();
    memcpy (MsgBody__+2, file_name.c_str(), len + 1);
    MsgBody__[len+3] = cmd;

    byte tran_id = GenTranNbr();

    try {
        SendPBPacket();
        pbuf__->readFromDevice();
    } 
    catch (CommException& ce) {
        Category::getInstance("BMP5")
                 .error("Communication error during Control File transaction");
        throw;
    }

    while (packetQueue__->size()) {
        Packet pack = packetQueue__->front();
        stat = ParsePakBusPacket (pack, 0x9e, tran_id);
        if (stat) {
            PacketErr ("Control File Transaction", pack, stat);
        }
        else {
	        resp_code = (byte)*(pack.begPacket + 11);
            if (!resp_code) {
                hold_off = (int) PBDeserialize ((byte *)pack.begPacket + 12, 2);
            }
        }

        packetQueue__->pop_front ();
    }

    if (resp_code == 0x00) {
        sleep (hold_off);
	    return SUCCESS;
    }
    else {
	    return resp_code;
    }
} 

/**
 * Function to retrieve available status information from the datalogger.
 * This function retrieves information about the datalogger, its operating 
 * system, and the currently executing program. Upon successful execution,
 * the SetProgStats() function is called to store the programming information
 * in the TableDataManager member. The information is required for generating the 
 * data file header.
 *
 * @param security_code: Security code of the datalogger, zero by default.
 * @return Throws AppException on failure;
 */
void 
BMP5Obj :: GetProgStats (uint2 security_code) throw (ParseException)
{
    int         stat;
    DLProgStats prog_stat;
    byte        resp_code = 0x01;
    byte       *pack_ptr;

    Priority__ = 0x02;
    MsgType__  = 0x18;
    MsgBodyLen__ = 2;

    MsgBody__[0] = (byte)(security_code >> 8);     
    MsgBody__[1] = (byte)(security_code);

    byte tran_id = GenTranNbr();

    try {
        SendPBPacket();
        pbuf__->readFromDevice();
    } 
    catch (CommException& ce) {
        Category::getInstance("BMP5")
                 .error("Communication error during Programming Statistics transaction");
        throw;
    }

    while (packetQueue__->size()) {
        Packet pack = packetQueue__->front();
        stat = ParsePakBusPacket (pack, 0x98, tran_id);
        /* 
         * TODO
         * I'm seeing a signature error in parsing the response to the GetProgStats
         * transaction at some of the sites. It is not a random error. The same byte
         * sequence is returned. This should be investigated to determine if this
         * error is caused by a special character in the response. i.e. the driver
         * software transforming a special byte etc. 
         * However, for now - I'm changing the code to go ahead and parse the response
         * since this is concerned only with metadata. All transactions dealing with
         * actual/critical data will continue operating in a strict mode.
         */
        if (stat) {
            Category::getInstance("BMP5").warn("Packet error in Programming Statistics transaction");
            PacketErr ("Get Programming Statistics Transaction", pack, stat);
        }
        else { 

	    resp_code = (byte)*(pack.begPacket + 11);
            if (!resp_code) {
                pack_ptr = (byte *)(pack.begPacket + 12);

                prog_stat.OSVer = GetVarLenString (pack_ptr); 
                pack_ptr += prog_stat.OSVer.size() + 1;
  
                prog_stat.OSSig = (uint2) PBDeserialize (pack_ptr, 2);
                pack_ptr += 2;

                prog_stat.SerialNbr = GetVarLenString (pack_ptr); 
                int serialNumber = atoi(prog_stat.SerialNbr.c_str());

                if ((serialNumber == 0) && (errno == EINVAL)) {
                    prog_stat.SerialNbr = "Unknown";
                }
                pack_ptr += prog_stat.SerialNbr.size() + 1;

                prog_stat.PowUpProg = GetVarLenString (pack_ptr); 
                pack_ptr += prog_stat.PowUpProg.size() + 2;

                prog_stat.ProgName = GetVarLenString (pack_ptr); 
                pack_ptr += prog_stat.ProgName.size() + 1;
  
                prog_stat.ProgSig = (uint2) PBDeserialize (pack_ptr, 2);
                tblDataMgr__->setProgStats (prog_stat);
            }
        } 

        packetQueue__->pop_front ();
    }

    if (resp_code != 0x00) {
	throw ParseException(__FILE__, __LINE__,
                "Failed to obtain programming statistics information"); 
    }
    return;
}

/**
 * Function to fetch a record from a specified table on the data logger.
 * @param tbl_ref: Reference to the Table structure for the table to collect
 *         data from.
 * @param fs: Output file stream to write the data files. If the output stream
 *         hasn't been opened, it will be opened in TableDataManager.storeRecord().
 *         Once open, the stream will retain its state between successive calls.
 * @param start_mode: Possible options (GET_LAST_REC|INQ_REC_INFO), 
 *         (GET_LAST_REC|STORE_DATA) or (GET_DATA_RANGE|STORE_DATA). The 
 *         constants are defined in pakbus.h. Other collection modes proposed
 *         in the PakBus manual are not supported.
 * @param record_size: Size of the record to collect. 
 * @param P1, P2: If collecting in GET_LAST_REC mode, they would be 1 and 0. 
 *         Else, P1 and P2 would refer to range of records to collect. It is
 *         prudent to limit the requested number of records so that the
 *         response won't exceed maximum packet size limit. 
 * @param span: Span of a datafile in seconds.
 * @return a RecordStat structure. It's recordTime member is set to the time
 *         of the first record returned in the last query exchange. The count
 *         member is set to -1 on failure. 
 *         If using GET_LAST_REC, count returns the last record number.
 *         For GET_DATA_RANGE this returns the number of collected records.
 */
RecordStat 
BMP5Obj :: get_records (Table& tbl_ref, byte start_mode, int record_size, 
        uint4 P1, uint4 P2, int span)
{
    uint4  beg_rec_nbr = 0xffffffff;
    NSec   beg_rec_time;
    uint4  byte_offset;
    byte   collect_mode = start_mode & 0x0f;
    byte   store_mode   = start_mode & STORE_DATA;
    int    data_len = 0;
    byte   frag_record = 0;
    uint2  num_recs = 0;
    Packet pack;
    int    pack_data_len;
    bool   pending = false;
    int    stat = SUCCESS;
    int    pack_stat;
    stringstream msgstrm;
    RecordStat recordStat;

    // Make sure this collection mode is implemented
    if ((collect_mode != 0x05) && (collect_mode != 0x06)) {
        msgstrm << "Unknown collection mode : " 
                << setfill('0') << setw(2) << hex << byte2int(collect_mode);
        Category::getInstance("BMP5").error(msgstrm.str());
        msgstrm.str("");
        return recordStat;
    }

    do {
        byte tran_id = GenTranNbr();
        try {
            sendCollectionCmd (collect_mode, tbl_ref, P1, P2); 
            pbuf__->readFromDevice();
        } 
        catch (CommException& ce) {
            Category::getInstance("BMP5")
                     .error("Communication error during collect transaction");
            throw;
        }
        
        while (packetQueue__->size()) {
            pack = packetQueue__->front();

            if ((pack_stat = ParsePakBusPacket (pack, 0x89, tran_id))) {
                stat = ((pack_stat == FAILURE)||((pack_stat & 0x0b) == 0x0b)) 
                        ? FAILURE:SUCCESS;
                PacketErr ("get_record::ParsePakBusPacket", pack, pack_stat);
                packetQueue__->pop_front();
                continue;
            }

            /* Check the data packets for problems typical with a data
             * packet */
            if ((stat = test_data_packet (tbl_ref, pack))) {
                PacketErr ("get_record::test_data_packet", pack, stat);
                packetQueue__->pop_front();
                continue;
            }

            /* Obtain the beginning record number and determine if the 
             * data packet contains a fragmented record */
            beg_rec_nbr = PBDeserialize ((byte *)(pack.begPacket+14), 4);
            frag_record = ( (*(pack.begPacket+18) & 0x80) >> 7 );
            
             /* Get the time of the first record */
            if (frag_record) {
                beg_rec_time = parseRecordTime((byte *)(pack.begPacket+22));
            }
            else {
                beg_rec_time = parseRecordTime((byte *)(pack.begPacket+20));
            }

            if (frag_record) {
                byte_offset =  PBDeserialize ((byte *)(pack.begPacket+18), 4);
                byte_offset &= 0x7fffffff; 
                pack_data_len = (pack.endPacket-4) - (pack.begPacket+30) + 1;
                // Copy data from the packet to the buffer
                memcpy ((char*)(dataBuf__+byte_offset), (char*)(pack.begPacket+22), 
                        pack_data_len); 

                // Set parameters for the next "collect" request
                collect_mode = 0x08;
                P1 = beg_rec_nbr;
                P2 = pack_data_len + byte_offset;

                // In case the data record contains variable length 
                // fields, I'm assuming that a packet smaller than 512
                // bytes will indicate that the last data packet for a
                // record has been received.

                if (record_size == -1) {
                    if (pack_data_len < 512) {
                        if (store_mode) {
                            stat = store_data (dataBuf__, tbl_ref, beg_rec_nbr, 
                                    1, span); 
                            if (SUCCESS == stat) {
                                num_recs = 1;
                            }
                        }
                        pending = false;
                    }
                    else {
                        pending  = true;
                    }
                }
                else {
                    data_len += pack_data_len; 
                    if (data_len >= record_size) {
                        if (store_mode) {
                            stat = store_data (dataBuf__, tbl_ref, beg_rec_nbr, 
                                    1, span); 
                            if (SUCCESS == stat) {
                                num_recs = 1;
                            }
                        }
                        pending = false;
                    }
                    else {
                        pending = true;
                    }
                }
            }
            else {
                // We are not dealing with a fragmented record
                if (store_mode) {
                    num_recs = (uint2) PBDeserialize ((byte *)(pack.begPacket+18), 2);
                    num_recs &= 0x7fff;
                    stat = store_data ((byte *)(pack.begPacket+20), tbl_ref, 
                               beg_rec_nbr, num_recs, span);
                }
                pending = false;
            }

            packetQueue__->pop_front();
        }

        if (stat != SUCCESS) {
            break;
        }
    } while (pending);

    if (stat != SUCCESS) {
        return recordStat;
    }

    if (store_mode) {
        recordStat.count = frag_record ? 1 : num_recs;
    }
    else {
        recordStat.count = beg_rec_nbr;
        recordStat.recordTime = beg_rec_time; 
    }

    return recordStat;
}

/**
 * Test a packet received in response to "Collect Data" transaction for errors.
 * @param tbl_ref: Reference to the table structure that corresponds to the 
 *                 data table.
 * @param pack: Reference to the PakBus packet to test.
 * @return SUCCESS | FAILURE
 */
int 
BMP5Obj :: test_data_packet (Table& tbl_ref, Packet& pack) throw (AppException)
{
    byte* ptr = (byte *)(pack.begPacket + 11);

    // The response should be larger than 12 bytes to contain any record
    if (pack.endPacket < (pack.begPacket + 10)) {
        Category::getInstance("BMP5")
                .warn("Invalid response - data packet smaller than 12 bytes.");
        return FAILURE;
    }

    // Check the response code
    if (*ptr) {
        // *ptr == 0x01 | *ptr == 0x02 | *ptr == 0x07
        if (*ptr == 0x01) {
            throw InvalidTDFException (__FILE__, __LINE__,
                    "Collect Error : Permission Denied");
        }
        else if (*ptr == 0x02) {
            throw AppException (__FILE__, __LINE__,
                    "Collect Error : Insufficient resources");
        }
        else if (*ptr == 0x07) {
            throw AppException (__FILE__, __LINE__,
                    "Collect Error : Invalid TDF");
        }
        else {
            throw AppException (__FILE__, __LINE__,
                    "Collect Error");
        }
    }
    ptr++;

    // Get past the "Table Number" field
    int tbl_num_from_resp = (int) PBDeserialize (ptr, 2);
    ptr += 2;

    if (tbl_num_from_resp != tbl_ref.TblNum) {
        Category::getInstance("BMP5")
                .warn("No data available from table - " + tbl_ref.TblName);
        return FAILURE;
    }

    if (ptr >= ((byte *)pack.endPacket-2)) {
        Category::getInstance("BMP5")
                .warn("No data available from table - " + tbl_ref.TblName);
        return FAILURE;
    }
    return SUCCESS;
}
