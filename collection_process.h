/**
 * @file collection_process.h
 * Contains classes for modeling data collection processes.
 */

#ifndef COLLECTION_PROCESS_H
#define COLLECTION_PROCESS_H

#include <stdexcept>
#include <iostream>

#include <exception>
#include <string>
#include <sstream>
#include <log4cpp/Category.hh>
#include "pb5.h"
#include "init_comm.h"

using namespace std;

/**
 * DataCollectionProcess is an interface for implementing a generic process.
 */
class DataCollectionProcess {
public:
    virtual ~DataCollectionProcess() throw() {}
    /*
     * Initialize class members using command line arguments prior to execution.
     */
    virtual void init(int argc, char* argv[]) throw (exception) = 0;
    /*
     * Execute the process.
     */
    virtual void run() throw (exception) = 0;
    /*
     * This would provide cleanup functions while exiting on a signal receipt.
     */
    virtual void onExit() throw (exception) = 0;
    /* 
     * Print usage information to standard output.
     */
    virtual void printHelp() throw () = 0;
    /*
     * Print version information to standard output.
     */
    virtual void printVersion() throw () = 0;
};

/**
 * Implementation of the DataCollectionProcess interface for collectinng data from
 * a PakBus (2005) protocol based datalogger.
 */
class PB5CollectionProcess : public DataCollectionProcess {
public:
    PB5CollectionProcess();
    ~PB5CollectionProcess() throw();
    virtual void init(int argc, char* argv[]) throw (exception);
    virtual void run() throw (exception);
    virtual void onExit() throw ();
    virtual void printHelp() throw ();
    virtual void printVersion() throw ();

protected :
    void parseCommandLineArgs(int argc, char* argv[]) throw (exception);
    void configure() throw (AppException);
    void checkLoggerTime() throw (AppException);
    void initSession(int nTry) throw (AppException);
    void collect() throw (AppException);
    void closeSession() throw ();
    void exitHandler(int signum) throw ();

private:
    auto_ptr<DataSource> dataSource__;
    CommInpCfg       appConfig__;
    pakbuf           IObuf__;
    TableDataManager tblDataMgr__;
    PakCtrlObj       pakCtrlImplObj__;
    BMP5Obj          bmp5ImplObj__;

    string           lockFilePath__;
    bool             optDebug__;
    bool             optCleanAppCache__;
    bool             executionComplete__;
    bool             loggerTimeCheckComplete__;
    stringstream     msgstrm;
};

#define PB5_APP_NAME "PbCdlComm"
// #define PB5_APP_VERS "1.3.5 (2008/07/28)"
// #define PB5_APP_VERS "1.3.6 (2009/07/16)"
// #define PB5_APP_VERS "1.3.8 (2010/05/12)" 
// 1.3.8 Lock file creation will happen in /tmp instead of /var/tmp

#define PB5_APP_VERS "1.3.9 (2010/08/31)" 
// Timecheck log format updated to match ARM Collection Format

/**
 * Singleton object for executing a data collection process.
 */
class DataCollectionProcessManager {
public:
    /*
     * Define supported process types here.
     */
    enum ProcessType { PB5 };

    /*
     * Return a reference to the DataCollectionProcessManager object.
     */
    static DataCollectionProcessManager& getInstance() 
    {
        static DataCollectionProcessManager dataCollectionProcessManager__;
        return dataCollectionProcessManager__;
    }

    /*
     * Execute a specific data collection process using the command-line args.
     * If the process takes it's input from a configuration file, it could be
     * specified through a command-line switch. Depending on the needs, a run()
     * function can be added that takes the procType and the config file path.
     *
     * @param procType: Process type specified using the ProcessType member. 
     * @param argc:     Stands for the argc as in main(int argc, char* argv[])
     * @param argv:     Stands for the argv as in main(int argc, char* argv[])
     */
    int run(ProcessType procType, int argc, char* argv[]) throw ()
    {
        int stat = 0;
        try {
             currExecProcess__ = getProcess(procType);
             if (currExecProcess__) {
                 currExecProcess__->init(argc, argv);
                 currExecProcess__->run();
                 delete currExecProcess__;
             }
        } 
        catch (exception& e) {
            cout << e.what() << endl;
            stat = 1;
        }
        catch (...) {
            cout << "Exception caught while executing collection process." << endl;
            stat = 1;
        }
        currExecProcess__ = NULL;
        return stat;
    }

    /**
     * Provides cleanup functionalities in a situation as signal-handling phase.
     */
    void cleanup() throw ()
    {
        if (NULL != currExecProcess__) {
            try {
                delete currExecProcess__;
            } catch(exception& e) {
                cout << e.what() << endl;
            }
        }
    }

protected:
    /**
     * Return an instance of the requested process type.
     * 
     * @param procType : Process type specified using the enum ProcessType member.
     */
    DataCollectionProcess* getProcess(DataCollectionProcessManager::ProcessType procType)
            throw (invalid_argument)
    {
        if (procType == DataCollectionProcessManager::PB5) {
            return new PB5CollectionProcess();
        }
        else {
            throw invalid_argument("Unknown process type specified");
        }
    }

private:
    /**
     * Constructor for the DataCollectionProcessManager.
     */
    DataCollectionProcessManager() 
    {
        currExecProcess__ = NULL;
    }

    /**
     * Handle to the currently executing process, useful for signal handling.
     */
    DataCollectionProcess* currExecProcess__;
};

#endif
