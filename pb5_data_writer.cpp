/**
 * @file pb5_data_writer.cpp
 * This will contain implementation of "writer" modules using various persistence mechanisms.
 * Presently only the AsciiWriter class is implemented. 
 */
#include <stdexcept>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <log4cpp/Category.hh>
#include "pb5.h"
#include "utils.h"
#include "collection_process.h"
using namespace std;
using namespace log4cpp;

/**
 * Accessor function for the TableDataWriterFactory object.
 * @return TableDataWriterFactory: Reference to the factory instance.
 */
TableDataWriterFactory& TableDataWriterFactory :: getInstance()
{
    static TableDataWriterFactory tblDataWriterFactory__;
    return tblDataWriterFactory__;
}

/**
 * Factory method for obtaining an instance of a TableDataWriter object.
 * 
 * @param type: Enum datatype to identify a writer object type. Currently
 *              supports the AsciiWriter object only.
 */
auto_ptr<TableDataWriter> TableDataWriterFactory :: getWriter(TDWtype type)
        throw (logic_error)
{
    switch (type) {
        case ASCII : return auto_ptr<TableDataWriter>(new AsciiWriter);
        default    : throw logic_error("Writer implementation unavailble");
    }
}

/**
 * Constructor for an AsciiWriter object.
 * 
 * @param datadir: Data directory for storing final datafiles.
 * @param filespan: Maximum timespan of a datfile. Defaults to 60 minutes.
 * @param sep: Seperator/delimiter character to use while writing data records.
 */
AsciiWriter :: AsciiWriter(string datadir, int filespan, char sep) :
    dataDir__(datadir), fileSpan__(filespan), seperator__(sep),
    recordCount__(0) 
{
    dataFileStream__.exceptions(ofstream::badbit | ofstream::failbit); 

    if (dataDir__.size() == 0) {
        dataDir__ = ".";
    }

    if (fileSpan__ < 0) {
        fileSpan__ = 3600;
    }
}

/**
 * Destructor ensures that the output datastream is closed.
 */
AsciiWriter :: ~AsciiWriter() 
{
    if (dataFileStream__.is_open()) {
        try {
            dataFileStream__.close();
        } 
        catch (ios_base::failure& fe) {
            Category::getInstance("AsciiWriter")
                     .error("Caught exception during closing filestream");
        } 
    }
}

/**
 * Convert the number of seconds (and nanoseconds) since 1990 into an 
 * equivalent timestamp. 
 *
 * @param timestamp: Pointer to the character string for storing the timestamp.
 * @param timeInfo:  Reference to the NSec structure containing time information.
 */

int AsciiWriter :: GetTimestamp (char *timestamp, const NSec& timeInfo)
{
    if (!timestamp) {
        return FAILURE;
    }

    struct tm *ptm;
    static int nano_precision = 3;
    static int factor = (10^(6-nano_precision));

    int nsecs = (int)(timeInfo.nsec/factor);
    // The sec component of the NSec datatype represents number of seconds 
    // since 1990.
    time_t  secs1970 = timeInfo.sec + SECS_BEFORE_1990;
    ptm = gmtime (&secs1970);
    
    if (ptm) {
        sprintf (timestamp, "\"%04d-%02d-%02d %02d:%02d:%02d.%d\"", 
                ptm->tm_year+1900, ptm->tm_mon+1, ptm->tm_mday, ptm->tm_hour, 
                ptm->tm_min, ptm->tm_sec, nsecs);
        return SUCCESS;
    }
    else {
        return FAILURE;
    }
}

/**
 * Function to obtain a timestamp as part of the filename based on a time 
 * specified using Campbell Scientific Time (measured from 1990).
 *
 * @param sample_time: Time measured from 1990.
 * @return Pointer to a character string containing the timestamp.
 */
string AsciiWriter :: getFileTimestamp (uint4 sample_time) 
        throw (invalid_argument)
{
    if (0 == sample_time) {
        throw invalid_argument("invalid sample time input to getFileTimestamp");
    }
    struct tm *ptm;
    time_t     secs1970 = (time_t) sample_time + SECS_BEFORE_1990;
    char file_timestamp[16];

    ptm = gmtime (&secs1970);
    if (ptm) {
        sprintf (file_timestamp, "%d%02d%02d_%02d%02d%02d", ptm->tm_year+1900,
                ptm->tm_mon+1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min,
                ptm->tm_sec);
    }
    else {
        throw invalid_argument("invalid sample time input to getFileTimestamp");
    }
    return file_timestamp;
}

void AsciiWriter :: initWrite(Table& tblRef) throw (StorageException)
{
    if (tblRef.NewFileTime) {
        openDataFile(tblRef, false);
    } 
    else {
        openDataFile(tblRef, true);
    }
}

void AsciiWriter :: reportRecordCount()
{
    if (recordCount__) {
        stringstream msg;
        msg << "Wrote " << recordCount__ << " records";
        Category::getInstance("AsciiWriter")
                 .debug(msg.str());
        recordCount__ = 0;
    }
}

void AsciiWriter :: processRecordBegin(Table& tbl_ref, int recordIdx, 
        NSec recordTime) 
{
    char timestamp[32];
    // GetTimestamp(timestamp, tbl_ref.LastRecordTime); 
    AsciiWriter::GetTimestamp(timestamp, recordTime);

    // if ( (tbl_ref.LastRecordTime.sec >= tbl_ref.NewFileTime) ) {
    if ( (recordTime.sec >= tbl_ref.NewFileTime) ) {
        // A new file needs to be created. If a file stream is already 
        // open for storing data, close it. 
      
        if (dataFileStream__.is_open() && tbl_ref.FirstSampleInFile) {
            dataFileStream__.close();
            reportRecordCount();
            moveRawFile (tbl_ref);
            openDataFile (tbl_ref, true);
        }

        // tbl_ref.FirstSampleInFile = tbl_ref.LastRecordTime.sec;
        tbl_ref.FirstSampleInFile = recordTime.sec;
        tbl_ref.NewFileTime = fileSpan__*((int)(recordTime.sec/fileSpan__)) 
                 + fileSpan__;
    }

    // Print the timestamp for the record followed by the values 
    // for each column in the table. 

    dataFileStream__ << timestamp << this->seperator__ << recordIdx;
}

void AsciiWriter :: processRecordEnd(Table& tbl_ref) 
{
    dataFileStream__ << endl;
    recordCount__ += 1;
}

void AsciiWriter :: finishWrite(Table& tblRef) throw (StorageException)
{
    if (dataFileStream__.is_open()) {
        try {
            dataFileStream__.close(); // close() can throw ios_base::failure too.
            if (dataFileStream__.bad()) {
                throw ios_base::failure("err");
            }
        } 
        catch (ios_base::failure& fe) {
            stringstream error;
            error << "Caught exception while closing datafile for : " 
                  << tblRef.TblName;
            Category::getInstance("AsciiWriter")
                     .notice(error.str());
            throw StorageException(__FILE__, __LINE__, "file closing error");
        }
        reportRecordCount();
    }
}

void AsciiWriter :: storeBool(const Field& var, bool flag)
{
   dataFileStream__ << this->seperator__ << flag;
}

void AsciiWriter :: storeFloat(const Field& var, float num)
{
   dataFileStream__ << this->seperator__ << num;
}

void AsciiWriter :: storeInt(const Field& var, int num)
{
    dataFileStream__ << this->seperator__ << num;
}

void AsciiWriter :: storeUint4(const Field& var, uint4 num)
{
   dataFileStream__ << this->seperator__ << num;
}

void AsciiWriter :: storeUint2(const Field& var, uint2 num)
{
   dataFileStream__ << this->seperator__ << num;
}

void AsciiWriter :: storeString(const Field& var, string& str)
{
   dataFileStream__ << this->seperator__ << "\"" << str << "\"";;
}

void AsciiWriter :: processUnimplemented(const Field& var)
{
   dataFileStream__ << this->seperator__ << "-9999";
}

/**
 * Function to create a new data file along with the header for the
 * particular table.
 * If opening the file in the append mode, the function checks to ensure that
 * the file exists/has non-zero size. If not, it switches to the "new" mode,
 * which includes creating the file and writing the header information based
 * on the corresponding Table structure.
 *
 * @param dataFileStream__: Reference to the output file stream to use.
 * @param tbl_ref:  Reference to the Table structure whose data is being stored.
 * @param new_file: Flag to indicate creation of a new file. False is used to
 *                  indicate append mode.
 * @return          SUCCESS if file was created. FAILURE if the file couldn't
 *                  be created.
 */
void AsciiWriter :: openDataFile (const Table& tbl_ref, bool new_file)
        throw (StorageException)
{
    int    file_stat;
    struct stat buf;
    bool   isSuccess(false);
    string tmp_file = this->getTableDataManager()
                          ->getDataOutputConfig().WorkingPath 
                        + "/.working/" + tbl_ref.TblName + ".tmp";

    if (!new_file) {
        file_stat = stat (tmp_file.c_str(), &buf);
        if (buf.st_size) {
            dataFileStream__.open (tmp_file.c_str(), ofstream::out | ofstream::app);
            isSuccess = dataFileStream__.is_open();
        }
        else {
            new_file = true;
        }
    }

    if (new_file) {
        dataFileStream__.open (tmp_file.c_str(), ofstream::out);
        isSuccess = dataFileStream__.is_open();
        writeHeader(tbl_ref);
    }
       
    if (!isSuccess) {
        string errMsg("Failed to open data file : ");
        errMsg += tmp_file;
        Category::getInstance("AsciiWriter").error(errMsg);
        throw StorageException(__FILE__, __LINE__, errMsg.c_str());
    }
}

string Field::getProperty(int infoType, int dim) const
{
    stringstream formattedPropertyValue;

    switch(infoType) 
    {
        case 1 : if (dim) {
                     formattedPropertyValue << "\"" <<  FieldName << "(" << dim << ")\"";
                 }
                 else {
                     formattedPropertyValue << "\"" <<  FieldName << "\"";
                 }
                 break;
        case 2 : formattedPropertyValue << "\"" <<  Unit << "\"";
                 break;
        case 3 : formattedPropertyValue << "\"" <<  Processing << "\"";
                 break;
        default: throw logic_error("Unknown field property queried"); 
    }
    return formattedPropertyValue.str();
}

void AsciiWriter :: printHeaderLine(const char* prefix, 
        const vector<Field>& fieldList, int infoType)
{
    int dim;
    vector<Field>::const_iterator fieldItr;

    if ((fieldList.size() == 0) || ((infoType < 1) || (infoType > 3))) {
        return;
    }

    fieldItr = fieldList.begin();
    dataFileStream__ << prefix;

    for (fieldItr = fieldList.begin(); fieldItr != fieldList.end(); 
            fieldItr++){
        if ( (fieldItr->Dimension > 1) && ( (fieldItr->FieldType != 11) &&
                (fieldItr->FieldType != 16) ) ) {
            for (dim = 1; dim <= (int)fieldItr->Dimension; dim++) {
               dataFileStream__ << fieldItr->getProperty(infoType, dim);
            }
        }
        else {
           dataFileStream__ << fieldItr->getProperty(infoType, 0);
        }
    }

   dataFileStream__ << endl;
   return;
}

void AsciiWriter :: writeHeader(const Table& tbl_ref)
{
    const vector<Field>& fieldList = tbl_ref.field_list;

    const TableDataManager* tblDataMgr = this->getTableDataManager();

    if (NULL == tblDataMgr) {
        throw runtime_error("NULL TableDataManager member in AsciiWriter!");
    }

    const DataOutputConfig& dataOutputConfig = tblDataMgr->getDataOutputConfig();
    const DLProgStats& dlProgStats = tblDataMgr->getProgStats();

    //////////////////////////////////////////////////////
    // Print the file header : File format type, Station
    // name, Datalogger type, serial number, OS Version,
    // Datalogger program name, Datalogger program 
    // signature and the table name
    //////////////////////////////////////////////////////

   dataFileStream__ << "\"TOA5\",\"" << dataOutputConfig.StationName << "\",\""
                                     << dataOutputConfig.LoggerType << "\",\""
                                     << dlProgStats.SerialNbr << "\",\"" 
                                     << dlProgStats.OSVer << "\",\""
                                     << dlProgStats.ProgName << "\",\"" 
                                     << dlProgStats.ProgSig << "\",\"" 
                                     << tbl_ref.TblName << "\",\"" 
                                     << PB5_APP_NAME << "-" 
                                     << PB5_APP_VERS << "\"" << endl;

    // Print field names
    printHeaderLine("\"TIMESTAMP\",\"RECORD\",", fieldList, 1);

    // Print field unit
    printHeaderLine("\"TS\",\"RN\",", fieldList, 2);

    // Print processing type for each field
    printHeaderLine("\"\",\"\",", fieldList, 3);

    return;
}

void AsciiWriter :: flush(const Table& tblRef)
{
    if(dataFileStream__.is_open()) { 
        dataFileStream__.close();
    }
    moveRawFile(tblRef);
}

/**
 * Function to rename the temporary datafile to one with appropriate timestamp.
 * It also moves the file from the <working_path>/.working to <working_path> 
 * directory. 
 * Throws AppException if there was an error in building the target datafile 
 * name or if the call to rename() failed.
 *
 * @param tbl_ref: Reference to the Table structure that the data corresponds to.
 */
void AsciiWriter :: moveRawFile (const Table& tbl_ref) throw (StorageException)
{
    stringstream logmsg;

    const DataOutputConfig& dataOutputConfig = this->getTableDataManager()
                                     ->getDataOutputConfig();

    string tmpDatafilePath(dataOutputConfig.WorkingPath);
    string finalDatafilePath(dataOutputConfig.WorkingPath);

    tmpDatafilePath.append("/.working/")
                   .append(tbl_ref.TblName)
                   .append(".tmp");

    try {
        finalDatafilePath.append("/")
                     .append(tbl_ref.TblName)
                     .append(".")
                     .append(getFileTimestamp(tbl_ref.FirstSampleInFile))
                     .append(".raw");
    } 
    catch(invalid_argument& iae) {
        return;
    }

    struct stat tmpFileStat;
    int status = stat(tmpDatafilePath.c_str(), &tmpFileStat);
    
    if (0 != status) {
        logmsg << "Failed to validate file size for " << tmpDatafilePath;
        Category::getInstance("AsciiWriter")
                 .warn(logmsg.str());
        logmsg.str("");
    }
    else {
        if(tmpFileStat.st_size == 0) {
            logmsg << "Removing zero-length temporary file : " << tmpDatafilePath;
            Category::getInstance("AsciiWriter")
                     .notice(logmsg.str());
            unlink(tmpDatafilePath.c_str());
            return;
        }
    }

    status = rename(tmpDatafilePath.c_str(), finalDatafilePath.c_str());

    struct stat fileStat;

    if (0 == status) {
        stat(finalDatafilePath.c_str(), &fileStat);
        logmsg << "Created : " << finalDatafilePath << " ("
               << fileStat.st_size << " bytes)";
        Category::getInstance("AsciiWriter")
            .info(logmsg.str());
    }
    else {
        stat(tmpDatafilePath.c_str(), &fileStat);
        logmsg << "Failed to rename " << tmpDatafilePath << "("
               << fileStat.st_size << " bytes) to " << finalDatafilePath;

        Category::getInstance("AsciiWriter")
                .error(logmsg.str());
        throw StorageException(__FILE__, __LINE__,
                logmsg.str().c_str());
    }
    return;
}

