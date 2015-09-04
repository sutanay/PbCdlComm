/**
 * @file init_comm.h
 * Collection of classes to perform actions during the application startup.
 */
/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: init_comm.h,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:54:46 $
 *   $State: Exp $
 *****************************************************/

#ifndef INIT_COMM_H
#define INIT_COMM_H
#include <string>
#include <vector>
#include <stdexcept>
#include "pb5.h"
using namespace std;

/** 
 * Abstract class for establishing communication with the target device.
 * It provides the interface that child classes must implement using 
 * functions specific to the communication mode (TCP/IP, RS232 etc).
 */
class DataSource {
    public :
        enum Type { UNKNOWN, RS232, TCP };
        DataSource(DataSource::Type type) : type__(type) {}
        static DataSource* createDataSource(const string& connectionString);
        static DataSource* decorate(DataSource* dataSource, 
                const string& connectionString);
        virtual int    connect() throw (CommException) = 0;
        virtual bool   disconnect() throw (CommException) = 0;
        virtual bool   isOpen() = 0;
        virtual string getConnInfo() = 0;
        virtual string getAddress() = 0;
        virtual void   setConnInfo(const string& arg) = 0;
        virtual bool   retryOnFail() { return false; }
        virtual string getLockId() = 0;
        string         getLockFileName(const char *AppName) throw (AppException);
        virtual ~DataSource () {};
        DataSource::Type getType() { return type__; } 
    private:
        DataSource::Type type__;
};


/**
 * Implementation of the DataSource interface that models the connection
 * to a serial device.
 */
#define DEFAULT_BAUD  115200
#define DEFAULT_VTIME 10
#define NUM_MAX_RETRY 8

class SerialConn : public DataSource {
    public :
        explicit SerialConn (const string& addr, int speed=DEFAULT_BAUD, 
                int vtime=DEFAULT_VTIME);
        ~SerialConn ();
        virtual int    connect() throw (CommException);
        virtual bool   disconnect() throw (CommException);
        virtual bool   isOpen() { return (fd__ > 0) ? true : false; }
        virtual string getConnInfo();
        virtual void   setConnInfo(const string& arg);
        virtual string getLockId();
        string  getAddress() { return portAddr__; }
        void    setPortName(string portName);
        int     getBaudRate() { return baudRate__; }
        void    setBaudRate(int baudRate);
        int     getVtime() { return vtime__; }
        void    setVtime(int vtime);
        virtual bool   retryOnFail();

    private :
        string portAddr__;
        int    baudRate__;
        int    fd__;
        int    vtimeArray__[7];
        int    vtimeIndex__;
        int    vtime__;
};


/**
 * This class loads the application configuration from a XML file and
 * provides access to data structures containing the configuration information.
 */
class CommInpCfg {
    public :
        CommInpCfg();
        ~CommInpCfg();

        void setWorkingPath (const string& working_path);
        void loadConfig (const char *cfg_file) throw (AppException);
        /** Returns string describing connection type. */
        // string&  getConnType () { return conn_type; };
        /** Returns a reference to the DataOutputConfig structure containing data
            output options. */
        const DataOutputConfig& getDataOutputConfig () { return dataOpt__; };   
        /** Returns a reference to the connection object.*/
        auto_ptr<DataSource> getDataSource (const string& connectionString) 
                throw (AppException);
        /** Returns a reference to the PBAddr structure containing PakBus
            address information. */
        PBAddr&  getPakbusAddr () { return pbAddr__; };
        void     dirSetup() throw (AppException);
        int      redirectLog();

    protected :
        void loadCollectionConfig (const char *filename) 
                throw (AppException);
        void loadSerialConfig (const xmlNodePtr node) 
                throw (AppException);
        void loadDataOutputConfig (const xmlNodePtr node) throw (AppException);
        void loadPakbusConfig (const xmlNodePtr node) 
                throw (AppException);

    private :
        auto_ptr<DataSource> dataSource__;
        DataOutputConfig  dataOpt__;
        PBAddr   pbAddr__;
};

#endif
