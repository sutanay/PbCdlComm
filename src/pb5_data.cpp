/** 
 * @file pb5_data.cpp
 * Implements functionalities for data storage and metadata management.
 */

/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: pb5_data.cpp,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:55:19 $
 *   $State: Exp $
 *****************************************************/

#include <memory>
#include <typeinfo>
#include <algorithm>
#include <iterator>
#include <fstream>
#include <sstream>
#include <string>
#include <cmath>
#include <time.h>
#include <unistd.h>
#include <libxml2/libxml/parser.h>
#include <libxml2/libxml/tree.h>
#include <log4cpp/Category.hh>
#include "pb5.h"
#include "utils.h"
using namespace std;
using namespace log4cpp;

/**
 * Operator += for NSec structure
 * 
 * @param timeval The operand
 */
void NSec :: operator+=(NSec& timeVal)
{
    this->sec += timeVal.sec;
    uint4 tmp = this->nsec + timeVal.nsec;
    if (tmp >= 1E9) {
        this->sec += 1;
        this->nsec = tmp - (uint4) 1E9;
    }
}

int nseccmp(const NSec& t1, const NSec& t2)
{
    if (t1.sec < t2.sec) {
        return -1;
    }
    else if (t1.sec > t2.sec) {
        return 1;
    }
    else {
        if (t1.nsec == t2.nsec) {
            return 0;
        }
        else if (t1.nsec < t2.nsec) {
            return -1;
        }
        else {
            return 1;
        }
    }
}

/**
 * Function to read in a variable length string from a bytestream.
 * Returns a string from the location in a byte sequence as pointed
 * by *str_ptr. 
 *
 * @param str_ptr: Pointer to the byte sequence.
 * @return String extracted from str_ptr.
 */
string GetVarLenString(const byte *str_ptr)
{
    const char *beg = (const char *) str_ptr;
    int count = 0;
    byte *ptr = (byte *)str_ptr;

    while (*ptr++ != 0x00) {
        count++;
    }
    string str(beg, count);
    return str;
}

/**
 * Function to read in a fixed length string from a bytestream.
 * Returns a string from the location in a byte sequence as pointed
 * by *str_ptr. 
 *
 * @param str_ptr: Pointer to the byte sequence.
 * @param var: Field for which the string is being extracted, dimension
 *             information stored in the Field structure is used.
 * @return String extracted from str_ptr.
 */
string GetFixedLenString (const byte *str_ptr, const Field& var)
{
    if (NULL == str_ptr) {
        return "";
    }

    int   count = 0;
    char *ptr = (char *)str_ptr;
    char *buf = new char[var.Dimension+1];

    while ( (count < (int)var.Dimension) && 
           ((*ptr != 0x0d) && (*ptr != '\n') && (*ptr != '\0')) ) {
        buf[count++] = *ptr++;
    }
    buf[count] = '\0';

    string str(buf);
    delete [] buf;
    return str;
}

/**
 * Function to convert a bit pattern to the equivalent floating
 * point number following IEEE-754 standard specifications.
 * The most significant bit -> sign, next 8 bits is the exponent
 * and the rest is the mantissa.
 * http://java.sun.com/j2se/1.4.2/docs/api/java/lang/Float.html
 * is a good place for more information about such functions.
 *
 * @param bits: Input bit pattern represented as a unsigned integer.
 * @return float: equivalent floating point number.
 */
float intBitsToFloat (uint4 bits)
{
    int s = ((bits >> 31) == 0) ? 1 : -1; 
    int e = ((bits >> 23) & 0xff);
    int m = (e == 0) ? 
        (bits & 0x7fffff) << 1 :
        (bits & 0x7fffff) | 0x800000;
    return (s*m*pow(2.0, (e-150)));
}

/**
 * Function to extract floating point number from low resolution
 * final storage format.
 * This function doesn't inspect the input bit pattern to find out
 * if the input bytes are part of a special number as a record ID,
 * or if they are part of a 3/4 byte floating point number. If the
 * absolute value of the number extracted is > 6999, -9999 is 
 * returned.
 *
 * @param unum: Input bytes represented as unsigned short.
 * @return Equivalent floating point number, -9999 on overflow.
 */ 
float GetFinalStorageFloat (uint2 unum)
{
    int   s = (unum >> 15) ? -1 : 1;
    int   factor = (unum & 0x6000) >> 13;
    float abs_val = pow (10.0, -1*factor) * (unum & 0x1fff);
    
    if (abs_val > 6999.0) {
        return -9999;
    }
    else {
        return (s*abs_val);
    }
}

/**
 * Constructor for the TableDataManager class. 
 *
 * @param data_opt: Reference to the DataOutputConfig structure that contains
 *                  various information for generating file headers. 
 */
TableDataManager :: TableDataManager () : tblDataWriter__(new AsciiWriter)
{ 
    tblDataWriter__->setTableDataManager(this);
}

TableDataWriter* TableDataManager :: getTableDataWriter()
{
    return tblDataWriter__.get();
}

void TableDataManager :: setTableDataWriter(TableDataWriter* dataWriter)
{
    tblDataWriter__.reset(dataWriter);
    tblDataWriter__->setTableDataManager(this);
    return; 
}

const DLProgStats& TableDataManager :: getProgStats() const
{
    return dataLoggerProgStats__;
}

void TableDataManager :: setProgStats(DLProgStats& progStats)
{
    dataLoggerProgStats__ = progStats;
    return; 
}

const DataOutputConfig& TableDataManager :: getDataOutputConfig() const
{
    return dataOutputConfig__;
}

void TableDataManager :: setDataOutputConfig(const DataOutputConfig& dataOpt)
{
    dataOutputConfig__ = dataOpt;
    tableList__.reserve(dataOpt.Tables.size()+2);
    return; 
}

/**
 * Destructor for the TableDataManager class.
 * For each table structure build from the table definition file, it
 * stores the currently available information for initiating the next
 * data collection and next data file creation.
 */
TableDataManager :: ~TableDataManager ()
{ 
    Category::getInstance("TableDataManager")
             .debug("Saving history for all collected tables.");
    saveTableStorageHistory();
}

/**
 * Function to store the storage history for each table found in the TDF file.
 */
void TableDataManager :: saveTableStorageHistory()
{
    ofstream tinfoFs;
    string   tinfoFile;

    for (int count = 0; count < (int)tableList__.size(); count++) {

        tinfoFile.append(dataOutputConfig__.WorkingPath)
                 .append("/.working/info.")
                 .append(tableList__[count].TblName);

        tinfoFs.open (tinfoFile.c_str(), ofstream::out);

        if (tinfoFs.is_open()) {
            tinfoFs << "# NextRecord, LastRecordTime, NewFileTime, TimeOfFirstSampleInFile" << endl
                    << tableList__[count].NextRecord << endl
                    << tableList__[count].LastRecordTime.sec << " " 
                    << tableList__[count].LastRecordTime.nsec << endl
                    << tableList__[count].NewFileTime << endl
                    << tableList__[count].FirstSampleInFile << endl;
            tinfoFs.close();
        }
        else { 
            Category::getInstance("TableDataManager")
                     .error("Failed to store collection state for " + 
                              tableList__[count].TblName);
        }
        tinfoFile.clear();
    }
}

/**
 * Function to construct Table structure information from the Table 
 * Definitions File.
 * It builds and maintains the table information in a vector of Table
 * structures in memory. Also the constructed table information is 
 * written to a XML file in the $DATA/.working directory.
 *
 * @return SUCCESS | FAILURE.
 */
int TableDataManager :: BuildTDF()
{
    ifstream TDFdata;
    tableList__.clear();
    string   conf_dir(dataOutputConfig__.WorkingPath);
    conf_dir += "/.working";
    string   tdf_file(conf_dir);
    string   xml_file(conf_dir);
    tdf_file += "/tdf.dat";
    xml_file += "/tdf.xml";

    TDFdata.open (tdf_file.c_str(), ios::binary);

    if (!TDFdata.is_open()) {
         Category::getInstance("TableDataManager")
                  .warn("Table definitions file does not exist : " +
                           tdf_file);
        return FAILURE;
    }
    TDFdata.seekg (0, ios_base::end);
    int len = TDFdata.tellg ();
    TDFdata.seekg (0, ios_base::beg);

    if (len == 0) {
        Category::getInstance("TableDataManager")
                 .error("No data available for parsing Table definitions");
        TDFdata.close();
        Category::getInstance("TableDataManager")
                 .info("Removing invalid table definition file : " + tdf_file);
        unlink(tdf_file.c_str());
        return FAILURE;
    }

    byte* tdf_data = NULL;

    try {
        tdf_data = new byte[len];
    } 
    catch(bad_alloc& bae) {
        Category::getInstance("TableDataManager")
                 .error("Failed to allocate buffer to parsing Table definitions");
        TDFdata.close();
        return FAILURE;
    }

    TDFdata.read ((char *)tdf_data, len);

    byte* ptr    = tdf_data;
    byte* endptr = tdf_data+len;
    int   table_num = 1; 

    fslVersion__ = *ptr++;
    
    while (ptr < endptr) {
        int nbytes = readTableDefinition (table_num, ptr, endptr);
        if (nbytes == -1) {
            Category::getInstance("TableDataManager")
                     .error("Failed to parse table definitions from : " + tdf_file);
            tableList__.clear();
            delete [] tdf_data;
            TDFdata.close();
            Category::getInstance("TableDataManager")
                     .info("Removing invalid table definition file : " + tdf_file);
            unlink(tdf_file.c_str());
            return FAILURE;
            break;
        }
        ptr += nbytes;
        table_num++;
    }
   
    // Clean up the buffer memory
    delete [] tdf_data;

    // Dump the table definitions into a XML file
    xmlDumpTDF ((char *)xml_file.c_str());

    // Load the storage history for various tables - information as last 
    // stored index etc.
    loadTableStorageHistory();

    return SUCCESS;
}

/**
 * Function to load the storage history for each table found in the TDF file.
 */
void TableDataManager :: loadTableStorageHistory()
{
    ifstream tinfo_fs;
    string   tinfo_file;
    char     buf[256];

    for (int count = 0; count < (int)tableList__.size(); count++) {

        tinfo_file = dataOutputConfig__.WorkingPath + "/.working/info." 
                + tableList__[count].TblName;

        tinfo_fs.open(tinfo_file.c_str(), ios_base::in);

        if (tinfo_fs.is_open()) {

            NSec lastRecordTime;

            tinfo_fs.getline (buf, 256);
            tinfo_fs >> tableList__[count].NextRecord
                     >> lastRecordTime.sec >> lastRecordTime.nsec
                     >> tableList__[count].NewFileTime
                     >> tableList__[count].FirstSampleInFile;
            tableList__[count].LastRecordTime = lastRecordTime;

            tinfo_fs.close();

            stringstream logmsg;
            logmsg << "Loaded history - " << tableList__[count].TblName 
                   << "(NextRecord:" << tableList__[count].NextRecord << ","
                   << "LastRecordTime:" << tableList__[count].LastRecordTime.sec 
                   << "." << tableList__[count].LastRecordTime.nsec << ","
                   << "NewFileTime:" << tableList__[count].NewFileTime << ","
                   << "FirstSampleInFile:" << tableList__[count].FirstSampleInFile
                   << ")";
            Category::getInstance("TableDataManager")
                     .debug(logmsg.str());
        }

        tinfo_file.clear();
    }
    return;
}

void
TableDataManager :: cleanCache ()
{
    Category::getInstance("TableDataManager")
            .debug("Removing table definitions file ...");
    string path(dataOutputConfig__.WorkingPath);
    path += "/.working/tdf.dat";
    unlink(path.c_str());
    
    path = dataOutputConfig__.WorkingPath;
    path += "/.working/tdf.xml";
    unlink(path.c_str());

    string tinfo_file;

    Category::getInstance("TableDataManager")
            .debug("Resetting data collection parameters");

    for (int count = 0; count < (int)tableList__.size(); count++) {
        tinfo_file = dataOutputConfig__.WorkingPath + "/.working/info." 
                + tableList__[count].TblName;
        // unlink (tinfo_file.c_str());
        tinfo_file = dataOutputConfig__.WorkingPath + "/.working/" 
                + tableList__[count].TblName + ".tmp";
        unlink (tinfo_file.c_str());
        tableList__[count].NextRecord = 0;
        tableList__[count].NewFileTime = 0;
        tableList__[count].FirstSampleInFile = 0; 
        tableList__[count].LastRecordTime.sec = 0; 
        tableList__[count].LastRecordTime.nsec = 0; 
    }
    tableList__.clear();
    return;
}

/** 
 * Read the structure of a table. A successful call returns the 
 * length of the segment for this table in the table definition file.
 *
 * @param table_num: Index for table number, beginning from 1. 
 * @param byte_ptr:  Pointer to the memory location to start reading next
 *                   table structure information.
 * @param endptr:    Pointer to the end of the buffer containing the 
 *                   table definition information. A read shouldn't be
 *                   performed beyond this limit.
 * @return Returns size of the table structure read in bytes. If the 
 * end of the byte stream was reached before completing the parsing,
 * -1 is returned.
 */
int TableDataManager :: readTableDefinition (int table_num, byte *byte_ptr, byte *endptr)
{
    Table tbl;
    byte *ptr = byte_ptr;

    if (ptr > endptr) return (-1);
    tbl.TblName = GetVarLenString (ptr);
    ptr += tbl.TblName.size ()+1;

    if (ptr > endptr) return -1;
    tbl.TblSize = PBDeserialize (ptr, 4);
    ptr += 4;

    if (ptr > endptr) return -1;
    tbl.TimeType = *ptr++;

    if (ptr > endptr) return -1;
    tbl.TblTimeInfo.sec = PBDeserialize (ptr, 4);
    ptr += 4;

    if (ptr > endptr) return -1;
    tbl.TblTimeInfo.nsec = PBDeserialize (ptr, 4);
    ptr += 4;

    if (ptr > endptr) return -1;
    tbl.TblTimeInterval.sec = PBDeserialize (ptr, 4);
    ptr += 4;

    if (ptr > endptr) return -1;
    tbl.TblTimeInterval.nsec = PBDeserialize (ptr, 4);
    ptr += 4;

    int nbytes = readFieldList (ptr, endptr, tbl);
    if (nbytes == -1) {
        return -1;
    }
    ptr += nbytes;
    
    int table_len = ptr - byte_ptr;
    tbl.TblSignature = CalcSig (byte_ptr, (uint4)(table_len), 0xaaaa);
    tbl.TblNum = table_num;    

    stringstream logmsg;

    if (tbl.TblName.size() > 0) {

        vector<Table>::const_iterator tblItr;
        bool dupFound(false);

        // Usually very few tables are stored on the logger, doesn't 
        // hurt to loop over.

        for (tblItr = tableList__.begin(); tblItr != tableList__.end();
                tblItr++) {
            if (tblItr->TblName.compare(tbl.TblName) == 0) {
                logmsg << "Duplicate entry found for [" << tbl.TblName 
                       << "] in table definitions file, ignoring later";
                Category::getInstance("TableDataManager")
                         .debug(logmsg.str());
                logmsg.str("");
                dupFound = true;
            }              
        }
       
        if (true != dupFound) {
            tableList__.push_back(tbl);
        }
    }
    else {
        stringstream logmsg;
        logmsg << "Ignoring " << table_len 
               << "-byte long entry in table definitions file with empty name string";
        Category::getInstance("TableDataManager")
                 .debug(logmsg.str());
    }
    return (table_len);
}

/**
 *  Reads in the list of fields from the bytestream. The field list
 *  is attached to a Table structure, also passed as the argument.
 *
 * @param byte_ptr:  Pointer to the memory location to start reading 
 *                   field list information.
 * @param endptr:    Pointer to the end of the buffer containing the 
 *                   table definition information. A read shouldn't be
 *                   performed beyond this limit.
 * @return Returns size of the field list read in bytes. If the end of
 * the buffer was reached before completing the parsing, -1 is returned.
 */

int TableDataManager :: readFieldList (byte *byte_ptr, byte *endptr, Table& Tbl)
{
    Field var;
    byte next_num;
    byte *ptr = byte_ptr;

    do
    {
        if (ptr > endptr) return -1;
        var.FieldType = *ptr++;
        var.FieldType &= 0x7f;

        if (ptr > endptr) return -1;
        var.FieldName = GetVarLenString (ptr);
        ptr += var.FieldName.size ()+1;
        ptr++;  // Get past the null byte as namelist terminator

        if (ptr > endptr) return -1;
        var.Processing = GetVarLenString (ptr);
        ptr += var.Processing.size ()+1;

        if (ptr > endptr) return -1;
        var.Unit = GetVarLenString (ptr);
        ptr += var.Unit.size ()+1;

        if (ptr > endptr) return -1;
        var.Description = GetVarLenString (ptr);
        ptr += var.Description.size ()+1;
        
        if (ptr > endptr) return -1;
        var.BegIdx  = PBDeserialize (ptr, 4);
        ptr += 4;

        if (ptr > endptr) return -1;
        var.Dimension = PBDeserialize (ptr, 4);
        ptr += 4;

        // Commenting out the if statement based on Dennis Oracheski's suggestion
        // if (var.Dimension > 1) {
        
            while (ptr < endptr) {

                uint4 num = PBDeserialize (ptr, 4);
                ptr += 4;

                if (num != 0x00) {
                    var.SubDim.push_back(num);
                }
                else{
                    break;
                }
            }
        // }
        // else {
            // ptr += 4;
        // }
        Tbl.field_list.push_back (var);
        next_num = PBDeserialize (ptr, 1);

    } while (next_num != 0);
    
    ptr += 1;
    return (ptr - byte_ptr);
}

/**
 * Function to dump the table definition format information into a 
 * XML file. 
 *
 * @param xmlDumpFile: Path of the file to write the table structure
 *                     information. 
 * @return SUCCESS | FAILURE;
 */
int TableDataManager :: xmlDumpTDF (char* xmlDumpFile)
{
    xmlDocPtr  doc;
    xmlNodePtr root;
    int        nbytes;
    vector<Table>::iterator tbl_itr;

    // Create new XML document with version 1.0 and create a 
    // root node named "TDF"

    doc = xmlNewDoc ((const xmlChar *)"1.0");
    root = xmlNewNode (NULL, (const xmlChar *)"TDF");
    xmlDocSetRootElement (doc, root);

    // Write each table information to the XML file

    for (tbl_itr = tableList__.begin(); tbl_itr != tableList__.end(); 
            tbl_itr++) {
        writeTableToXml (root, *tbl_itr);
    }

    // Save the document tree and free used memory

    nbytes = xmlSaveFormatFile (xmlDumpFile, doc, 1);
    xmlFreeDoc (doc);

    if (nbytes == -1) {
        return FAILURE;
    }
    else {
        return SUCCESS;
    }
}

/**
 * This function writes the structure information for each table to a
 * XML document. The name, record size, signature and information about
 * each field in the table is added to the document tree.
 *
 * @param doc_root: Pointer to the root of the XML document.
 * @param tbl: Reference to the table structure being written out.
 */
// TODO Test the occasional segmentation fault occuring in this function.
void TableDataManager :: writeTableToXml (xmlNodePtr doc_root, Table& tbl)
{
    xmlNodePtr  table_node;
    char        buf[64];
    vector<Field>::iterator field_itr;

    // Create a node for each table and set the name attribute
    if (!tbl.TblName.size()) {
        return;
    }
    table_node = xmlNewChild (doc_root, NULL, (const xmlChar *)"TABLE", NULL);

    // Set various attributes for the table node
    
    xmlSetProp (table_node, (const xmlChar *)"Name", 
            (const xmlChar *)tbl.TblName.c_str() );

    sprintf (buf, "%d", tbl.TblSize);
    xmlSetProp (table_node, (const xmlChar *)"Table_Size", 
            (const xmlChar *)buf);

    sprintf (buf, "%d", getRecordSize (tbl)); 
    xmlSetProp (table_node, (const xmlChar *)"Record_Size", 
            (const xmlChar *)buf);

    sprintf (buf, "%d", tbl.TblSignature);
    xmlSetProp (table_node, (const xmlChar *)"Signature", 
            (const xmlChar *)buf);

    sprintf (buf, "%d", (int)tbl.TimeType);
    xmlSetProp (table_node, (const xmlChar *)"Time_Type", 
            (const xmlChar *)buf);

    sprintf (buf, "%d.%ds", tbl.TblTimeInterval.sec, tbl.TblTimeInterval.nsec);
    xmlSetProp (table_node, (const xmlChar *)"Time_Interval", 
            (const xmlChar *)buf);

    // Now add information about each field in the table

    for (field_itr = tbl.field_list.begin(); field_itr != tbl.field_list.end(); 
            field_itr++) {
        writeFieldToXml (table_node, *field_itr);
    }
    return;
}

/**
 * This function creates a "field" node under a "table" node in the 
 * XML document tree. Name, Unit, Processing, Data Type, Description
 * and Dimension information for each field is written out.
 *
 * @param table_node: xmlNodePtr to which the field information will be
 *                    attached.
 * @param var:        Reference to the field structure being written out.
 */
void TableDataManager :: writeFieldToXml (xmlNodePtr table_node, Field& var)
{
    xmlNodePtr field_node;
    char       buf[128];

    field_node = xmlNewChild (table_node, NULL, (const xmlChar *)"Field", 
            NULL);

    xmlSetProp (field_node, (const xmlChar *)"Name", 
            (const xmlChar *)var.FieldName.c_str());

    if (var.Unit.length() > 0) {
        xmlSetProp (field_node, (const xmlChar *)"Unit", 
                (const xmlChar *)var.Unit.c_str());
    }
    
    if (var.Processing.length() > 0) {
        xmlSetProp (field_node, (const xmlChar *)"Processing", 
                (const xmlChar *)var.Processing.c_str());
    }
    
    xmlSetProp (field_node, (const xmlChar *)"Type", 
            (const xmlChar *)getDataType (var));
    
    if (var.Description.length() > 0) {
        xmlSetProp (field_node, (const xmlChar *)"Description", 
                (const xmlChar *)var.Description.c_str());
    }
    
    sprintf (buf, "%d", var.Dimension);
    xmlSetProp (field_node, (const xmlChar *)"Dimension", 
            (const xmlChar *)buf);
    return;
}

/** 
 * Function to get record size for a particular table structure.
 * 
 * @param tbl: Reference to table structure whose size is being queried.
 * @return Record size for that input table, -1 if the table contains a
 *             variable length member.
 */
int TableDataManager :: getRecordSize (const Table& tbl) 
{
    int field_size;
    int RecSize = 0;
    vector<Field>::const_iterator field_itr;

    for (field_itr = tbl.field_list.begin(); field_itr != tbl.field_list.end();
            field_itr++) {
        field_size = getFieldSize (*field_itr);
        if (field_size > 0) {
            RecSize += field_size;
        }
        else {
            return -1;
        }
    }

    return RecSize;
}

/**
 * Function to determine the maximum record size.
 */
int TableDataManager :: getMaxRecordSize() 
{
    vector<Table>::const_iterator tblItr;
    int maxTableSize = -1;
    int tableSize;

    for (tblItr = tableList__.begin(); tblItr != tableList__.end(); tblItr++) {
        tableSize = getRecordSize(*tblItr);
        maxTableSize = max(maxTableSize, tableSize);
    }
    return maxTableSize;        
}

/**
 * Function to obtain number of bytes allocated for a particular field in 
 * the record for a table.
 *
 * @param field: Referene to the Field structure being queried.
 * @return Field size (within record) in bytes, -1 if the field size is 
 *               variable of unknown.
 */
int TableDataManager :: getFieldSize (const Field& field)
{
    int field_type = (int) field.FieldType;
    int field_size = 0;

    switch (field_type) {
        case 1  :
            field_size = 1;
            break;
        case 2  :
            field_size = 2;
            break;
        case 3  :
            field_size = 4;
            break;
        case 4  :
            field_size = 1;
            break;
        case 5  :
            field_size = 2;
            break;
        case 6  :
            field_size = 4;
            break;
        case 7  :
            field_size = 2;
            break;
        case 8  :
            field_size = 4;
            break;
        case 9  :
            field_size = 4;
            break;
        case 10 :
            field_size = 1;
            break;
        case 11 :
            field_size = field.Dimension;
            break;
        case 12 :
            field_size = 4;
            break;
        case 13 :
            field_size = 6;
            break;
        case 14 :
            field_size = 8;
            break;
        case 15 :
            field_size = 3;
            break;
        case 16 :
            field_size = -1;
            break;
        case 17 :
            field_size = 1;
            break;
        case 18 :
            field_size = 8;
            break;
        case 19 :
            field_size = 2;
            break;
        case 20 :
            field_size = 4;
            break;
        case 21 :
            field_size = 2;
            break;
        case 22 :
            field_size = 4;
            break;
        case 23 :
            field_size = 8;
            break;
        case 24 :
            field_size = 4;
            break;
        case 25 :
            field_size = 8;
            break;
        case 27 :
            field_size = 2;
            break;
        case 28 :
            field_size = 4;
            break;
        default : 
            field_size = -1;
    }

    if (field_size > 0) {
        if (field.FieldType != 11) {
           field_size *= field.Dimension;
        } 
    }
    return field_size;
}

/**
 * Function to obtain a description of the data type for a particular field.
 *
 * @param var: Reference to the Field structure being queried.
 * @return Pointer to the character string containing the type description.
 */
const char* TableDataManager :: getDataType (const Field& var)
{
    int data_type = 0x000000ff & var.FieldType;
    switch (data_type) 
    {
        case 1 : 
            return "1-byte uint";
        case 2 : 
            return "2-byte unsigned integer (MSB first)";
        case 3 : 
            return "4-byte unsigned integer (MSB first)";
        case 4 : 
            return "1-byte signed integer";
        case 5 : 
            return "2-byte signed integer (MSB first)";
        case 6 : 
            return "4-byte signed integer (MSB first)";
        case 7 : 
            return "2-byte final storage floating point";
        case 15 : 
            return "3-byte final storage floating point - NOT IMPLEMENTED";
        case 8 : 
            return "4-byte final storage floating point (CSI format) - NOT IMPLEMENTED";
        case 9 : 
            return "4-byte floating point (IEEE standard, MSB first)";
        case 18 : 
            return "8-byte floating point (IEEE standard, MSB first) - NOT IMPLEMENTED";
        case 17 : 
            return "Byte of flags";
        case 10 : 
            return "Boolean value";
        case 27 : 
            return "Boolean value";
        case 28 : 
            return "Boolean value";
        case 12 : 
            return "4-byte integer used for 1-sec resolution time";
        case 13 : 
            return "6-byte unsigned integer, 10's of ms resolution - NOT IMPLEMENTED";
        case 14 : 
            return "2 4-byte integers, nanosecond time resolution (unused by CR23xx) - NOT IMPLEMENTED";
        case 11 : 
            return "fixed length string of lengh n, unused portion filled";
        case 16 : 
            return "variable length null-terminated string of length n+1";
        case 19 : 
            return "2-byte integer (LSB first) (unused by CR23xx) - NOT IMPLEMENTED";
        case 20 : 
            return "4-byte integer (LSB first) (unused by CR23xx) - NOT IMPLEMENTED";
        case 21 : 
            return "4-byte integer (LSB first) (unused by CR23xx) - NOT IMPLEMENTED";
        case 22 : 
            return "4-byte unsigned integer (LSB first) (unused by CR23xx) - NOT IMPLEMENTED";
        case 23 : 
            return "2 longs (LSB first), seconds then nanoseconds (unused by CR23xx) - NOT IMPLEMENTED";
        case 24 : 
            return "4-byte floating point (IEEE format, LSB first) (unused by CR23xx) - NOT IMPLEMENTED";
        case 25 : 
            return "8-byte floating point (IEEE format, LSB first) (unused by CR23xx) - NOT IMPLEMENTED";
        case 26 : 
            return "4-byte floating point value";
        default : 
            return "Unknown";
    }
}

/**
 * Function to obtain a reference to a particular Table structure contained in
 * the TableDataManager class.
 * The table list stored within the TableDataManager class is searched for a table
 * with the given name. Throws exception if table couldn't be found.
 *
 * @param TableName: string containing the table name to search for.
 * @return Reference to the appropriate table structure.
 */
Table& TableDataManager :: getTableRef (const string& TableName) throw (invalid_argument)
{
    vector<Table>::iterator tbl_itr;
    int  idx = 0;
    
    for (tbl_itr = tableList__.begin (); tbl_itr != tableList__.end (); tbl_itr++) {
        if (tbl_itr->TblName == TableName) {
            return tableList__[idx];
        }
        idx++;
    }
    stringstream logmsg;
    logmsg << "Failed to find information about [" << TableName 
           << "] among table definition file entries";
    throw invalid_argument(logmsg.str());
}

NSec parseRecordTime(const byte* data)
{
    NSec recordTime;
    recordTime.sec = PBDeserialize (data, 4);
    recordTime.nsec = PBDeserialize (data+4, 4);
    return recordTime; 
}

/**
 * This function extracts a record from a byte stream for a Table structure and 
 * writes that to a file using the specified output stream.
 * In addition to extracting records and writing data to file, this function also
 * ensures splitting of files on hour boundaries.
 *
 * @param fs: Reference to output file stream.
 * @param tbl_ref: Reference to corresponding Table strucrure.
 * @param data: Address of the pointer to the beginning of the byte sequence.
 * @param rec_num: Number of the record to store in file.
 * @param file_span: Span of a datafile in seconds
 * @return SUCCESS | FAILURE (If the data file couldn't be opened).
 */ 
int TableDataManager :: storeRecord (Table& tbl_ref, byte **data, 
        uint4 rec_num, int file_span, bool parseTimestamp) throw (StorageException)
{
    vector<Field>           field_list = tbl_ref.field_list;
    vector<Field>::const_iterator start, end, itr;
    NSec recordTime;

    start = field_list.begin();
    end   = field_list.end();

    try {
        if (parseTimestamp) {
            // tbl_ref.LastRecordTime = parseRecordTime(*data);
            recordTime = parseRecordTime(*data);
            *data += 8;
        } 
        else {
            // tbl_ref.LastRecordTime += tbl_ref.TblTimeInterval;
            recordTime = tbl_ref.LastRecordTime;
            recordTime += tbl_ref.TblTimeInterval;
        }
    
        tblDataWriter__->processRecordBegin(tbl_ref, rec_num, 
                recordTime);
        
        for (itr = start; itr < end; itr++) {
            if ((itr->FieldType == 11) || (itr->FieldType == 16)) {
                storeDataSample(*itr, data);
            }
            else {
                for (int dim = 0; dim < (int)itr->Dimension; dim++) {
                    storeDataSample(*itr, data);
                } 
            }
        }

        tblDataWriter__->processRecordEnd(tbl_ref);
       
        // Update state variables
        tbl_ref.LastRecordTime = recordTime;
        tbl_ref.NextRecord += 1;
    }
    catch (...) {
        stringstream errormsg;
        char timestamp[64];
        AsciiWriter::GetTimestamp(timestamp, recordTime);
        errormsg << "Failure in storing data record{\"id\":" 
                 << tbl_ref.NextRecord << ", \"timestamp\":"
                 << timestamp << "}";
        throw StorageException(__FILE__, __LINE__, errormsg.str().c_str());
    } 
    return SUCCESS; 
}

/**
 * Function to extract a sample for a particular field from a data record
 * and write it to an output file stream.
 *
 * @param fs:   Reference to the output file stream.
 * @param var:  Reference to the Field structure being extracted from data.
 * @param data: Address of the pointer to the memory where the data sample
 *              is stored.
 */
void TableDataManager :: storeDataSample (const Field& var, byte **data)
{
     uint4   unum = 0;
     uint2   unum2 = 0;
     int     num  = 0;
     string  str;
    
     switch (var.FieldType) 
     {
         case 7 : 
             // 2-byte final storage floating point - Tested with CR1000 data
             unum2 = (uint2) PBDeserialize (*data, 2);
             tblDataWriter__->storeFloat(var, GetFinalStorageFloat(unum2));
             *data += 2;
             break;

         case 6 : 
             // 4-byte signed integer (MSB first) - Tested with CR1000 data
             num   = (int)PBDeserialize (*data, 4);
             tblDataWriter__->storeInt(var, num);
             *data += 4;
             break;

         case 9 : 
             // 4-byte floating point (IEEE standard, MSB first) - Tested with 
             // CR1000 data
             num = PBDeserialize (*data, 4);
             tblDataWriter__->storeFloat(var, intBitsToFloat(num));
             *data += 4;
             break;

         case 10 : 
             // Boolean value - Tested with CR1000 data
             unum = PBDeserialize (*data, 1);
             tblDataWriter__->storeBool(var, unum & 0x80);
             *data += 1;
             break;

         case 16 : 
             // variable length null-terminated string of length n+1 - Tested 
             // with CR1000 data
             str = GetVarLenString (*data);
             tblDataWriter__->storeString(var, str);
             *data += str.size() + 1;
             break;

         case 11 : 
             // fixed length string of lengh n, unused portion filled 
             // with spaces/null - Tested with CR1000 data
             str = GetFixedLenString (*data, var);
             tblDataWriter__->storeString(var, str);
             *data += var.Dimension;
             break;

         case 1 : 
             // 1-byte uint
             unum = PBDeserialize (*data, 1);
             tblDataWriter__->storeUint4(var, unum);
             *data += 1;
             break;
         case 2 : 
             // 2-byte unsigned integer (MSB first)
             unum = PBDeserialize (*data, 2);
             tblDataWriter__->storeUint4(var, unum);
             *data += 2;
             break;
         case 3 : 
             // 4-byte unsigned integer (MSB first)
             unum = PBDeserialize (*data, 4);
             tblDataWriter__->storeUint4(var, unum);
             *data += 4;
             break;
         case 4 : 
             // 1-byte signed integer
             num  = (int)PBDeserialize (*data, 1);
             tblDataWriter__->storeInt(var, num);
             *data += 1;
             break;
         case 5 : 
             // 2-byte signed integer (MSB first)
             num = (int)PBDeserialize (*data, 2);
             tblDataWriter__->storeInt(var, num);
             *data += 2;
             break;
         case 18 : 
             // 8-byte floating point (IEEE standard, MSB first)
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 8;
             break;
         case 15 : 
             // 3-byte final storage floating point
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 3;
             break;
         case 8 : 
             // 4-byte final storage floating point (CSI format)
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         case 17 : 
             // Byte of flags
             unum = PBDeserialize (*data, 1);
             tblDataWriter__->storeUint4(var, unum);
             *data += 1;
             break;
         case 27 : 
             // Boolean value
             unum = PBDeserialize (*data, 1);
             tblDataWriter__->storeBool(var, unum & 0x80);
             *data += 1;
             break;
         case 28 : 
             // Boolean value
             unum = PBDeserialize (*data, 1);
             tblDataWriter__->storeBool(var, unum & 0x80);
             *data += 1;
             break;
         case 12 : 
             // 4-byte integer used for 1-sec resolution time
             unum = PBDeserialize (*data, 4);
             tblDataWriter__->storeUint4(var, unum);
             *data += 4;
             break;
         case 13 : 
             // 6-byte unsigned integer, 10's of ms resolution
             // Read a ulong, then mask out last 2 bytes
             unum = PBDeserialize (*data, 4);
             tblDataWriter__->storeUint4(var, unum);
             *data += 6;
             break;
         case 14 : 
             // 2 4-byte integers, nanosecond time resolution
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 8;
             break;
         case 19 : 
             // 2-byte integer (LSB first)
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 2;
             break;
         case 20 : 
             // 4-byte integer (LSB first)
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         case 21 : 
             // 2-byte unsigned integer (LSB first)
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 2;
             break;
         case 22 : 
             // 4-byte unsigned integer (LSB first)
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         case 24 : 
             // 4-byte floating point (IEEE format, LSB first)
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         case 25 : 
             // 8-byte floating point (IEEE format, LSB first)
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 8;
             break;
         case 23 : 
             // 2 longs (LSB first), seconds then nanoseconds
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 8;
             break;
         case 26 : 
             // 4-byte floating point value
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         default : 
             tblDataWriter__->processUnimplemented(var);
             logUnimplementedDataError(var);
    }
    return;
}

/**
 * Function to print out an error message in the log file if an unsupported
 * data type was found while collecting data for a table.
 * 
 * @param var: Reference to the Field structure with the unimplemented data
 *             type.
 */
void TableDataManager :: logUnimplementedDataError (const Field& var)
{
    static vector<string> err_field_list(100);
    static string last_err_field_name;

    if (last_err_field_name == var.FieldName) {
        return;
    }

    if (!err_field_list.empty()) {
        vector<string>::iterator result = find (err_field_list.begin(), 
                err_field_list.end(), var.FieldName); 
        if ( (result != err_field_list.end()) || 
                !(var.FieldName.compare(err_field_list.back())) ) {
            return;
        }
    }

    stringstream msgstrm;
    msgstrm << "ERROR in decoding data values for Field \"" << var.FieldName 
            << "\" [" << getDataType (var) << "]" << endl;
    Category::getInstance("TableDataManager")
            .info(msgstrm.str());
    err_field_list.push_back(var.FieldName);
    last_err_field_name = var.FieldName;
    return;
}

void TableDataManager :: flushTableDataCache(Table& tblRef)
{
    tblDataWriter__->flush(tblRef);
    tblRef.NewFileTime = 0;
    tblRef.FirstSampleInFile = 0;
}
