/** 
 * @file init_comm.cpp
 * Implements various startup activities such as loading configuration files, log management etc.
 */

/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: init_comm.cpp,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:55:19 $
 *   $State: Exp $
 *****************************************************/

#include <iostream>
#include <sstream>
#include <exception>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <libxml2/libxml/parser.h>
#include <libxml2/libxml/tree.h>
#include <log4cpp/Category.hh>
#include <log4cpp/Priority.hh>
#include <log4cpp/FileAppender.hh>
#include <log4cpp/Layout.hh>
#include <log4cpp/PatternLayout.hh>
#include "init_comm.h"
#include "serial_comm.h"
#include "utils.h"
using namespace std;
using namespace log4cpp;

DataSource* DataSource :: createDataSource(const string& connectionString)
{
    DataSource* dataSource = DataSource::decorate(NULL, connectionString);
    return dataSource;
}

DataSource* DataSource :: decorate(DataSource* dataSource, const string& connectionString)
{
    if (connectionString.find("tty") != string::npos) {
        if(dataSource && (dataSource->getType() != DataSource::RS232)) {
            return dataSource;
        }
        string port;
        int speed = 0;
        size_t pos = connectionString.find(",");

        if (string::npos != pos) {
            port = connectionString.substr(0, pos); 
            speed = atoi(connectionString.substr(pos+1).c_str());
        }
        else {
            port = connectionString;
        }
        if (dataSource) {
            SerialConn* serialConn = dynamic_cast<SerialConn*> (dataSource);
            if (serialConn) {
                serialConn->setPortName(port);
                if (speed) serialConn->setBaudRate(speed);
            }
        }
        else {
            dataSource = new SerialConn(port, speed);
        } 
    }
    return dataSource;
}

/**
 * Function to obtain a the name of the lock file for a given 
 * application.
 *
 * @param AppName: string containing name of an application.
 */
string DataSource :: getLockFileName (const char *AppName) throw (AppException)
{
    string lockFile ("/tmp/");
    if (!AppName || (strlen(AppName) > 256)) {
        throw AppException(__FILE__, __LINE__, "Invalid AppName");
    }
    lockFile += AppName; 
    lockFile += "-";
    lockFile += getLockId();
    lockFile += ".lck";
    return lockFile;
}

/**
 * Constructor for the SerialConn object.
 * 
 * @param addr: Pointer to character string containing the path 
 *              for the serial port.
 * @param speed: Baud rate specified as integer (i.e. 9600).
 */
// TODO How to reset vtimeIndex__
SerialConn :: SerialConn (const string& addr, int speed, int vtime) : 
    DataSource(RS232),
    portAddr__(addr), baudRate__(speed)
{
    if (speed <= 0) {
        baudRate__ = DEFAULT_BAUD;
    }
    vtimeArray__[0] = 2;
    vtimeArray__[1] = 5;
    vtimeArray__[2] = 10;
    vtimeArray__[3] = 20;
    vtimeArray__[4] = 30; 
    vtimeArray__[5] = 50;
    vtimeArray__[6] = 100;
    vtimeArray__[7] = 200;
    vtimeArray__[8] = 600;

    vtimeIndex__ = 2;
    setVtime(vtime);
}

/** 
 * Setter method for port name.
 */
void SerialConn :: setPortName(string portName)
{
    portAddr__ = portName;
}

/** 
 * Setter method for baud rate.
 */
void SerialConn :: setBaudRate(int baudRate)
{
    baudRate__ = baudRate;
}

/** 
 * Setter method for vtime.
 */
void SerialConn :: setVtime(int vtime) 
{
    vtime__ = (vtime < 2) ? 2 : vtime;

    switch (vtime) {
        case 2   : vtimeIndex__ = 0; break; 
        case 5   : vtimeIndex__ = 1; break; 
        case 10  : vtimeIndex__ = 2; break; 
        case 20  : vtimeIndex__ = 3; break; 
        case 30  : vtimeIndex__ = 4; break; 
        case 50  : vtimeIndex__ = 5; break; 
        case 100 : vtimeIndex__ = 6; break; 
        case 200 : vtimeIndex__ = 7; break; 
        case 600 : vtimeIndex__ = 8; break; 
        default:   int i; 
                   for (i = 0; i < 8; i++) {
                       if (vtimeArray__[i+1] > vtime__) {
                           break;
                       }
                   }
                   vtimeIndex__ = i;
                   break;
    }
}

bool SerialConn :: retryOnFail()
{
    if (vtimeIndex__ < NUM_MAX_RETRY) {
        vtimeIndex__++;
        vtime__ = vtimeArray__[vtimeIndex__];
        return true;
    }
    else {
        return false;
    } 
}

/**
 * A function to obtain a descriptive string about the connection, 
 * useful for writing to log.
 */
string SerialConn :: getConnInfo () 
{
    stringstream msg;
    msg << portAddr__ << " [baud(" << baudRate__ << "),vtime(" 
        << vtime__ << ")]";
    return msg.str();
}

/**
 * Function useful for setting connection parameters through command
 * line arguments.
 */
void SerialConn :: setConnInfo (const string& arg) 
{
    portAddr__ = arg;
}

/**
 * Function to extract the name of the port the serial device is 
 * connected to.
 *
 * @return Returns name of the port as a string. For example, if the 
 *         port address was "/dev/ttyS1", "ttyS1" would be returend.
 */
string SerialConn :: getLockId () 
{
    char dev[32];
    sscanf (portAddr__.c_str(), "/dev/%s", dev);
    return dev;
}

/**
 * Function to connect to the serial device.
 *
 * @return Returns the file descriptor on successful connection, else
 *         returns zero.
 */
int SerialConn :: connect () throw (CommException)
{
    stringstream msgstrm;
  
    fd__ = OpenComConfig((char *)portAddr__.c_str(), baudRate__, 0, 8, 1, 
                   vtime__);
  
    if(fd__ == -1){
        msgstrm << "Failed to connect to " << portAddr__ << endl;
        Category::getInstance("SerialConn")
                 .error(msgstrm.str());
        throw CommException(__FILE__, __LINE__, msgstrm.str().c_str());
    }
    else{
        msgstrm << "Successfully connected to device: " << getConnInfo();
        Category::getInstance("SerialConn")
                 .debug(msgstrm.str());
        return fd__;
    }
}

bool SerialConn :: disconnect()  throw (CommException)
{
    if (fd__ > 0) {
        CloseCom(fd__);
        fd__ = -1;
    }
    return true;
}

/**
 * Destructor for the SerialConn class.
 * It closes the serial port.
 */
SerialConn :: ~SerialConn ()
{
    if (fd__ != -1) {
        this->disconnect();
    }
}

/**
 * Function to set the working path. Can be used to override the option set
 * in the configuration file.
 * 
 * @param working_path: Directory name to set the working path.
 */
void CommInpCfg :: setWorkingPath (const string& working_path)
{
    dataOpt__.WorkingPath = working_path;
    return;
}

/**
 * Function for loading the serial port settings from the XML configuration file.
 *
 * @node: Pointer to the <SERIAL> node in the XML configuration file.
 */ 
void CommInpCfg :: loadSerialConfig (const xmlNodePtr node) 
        throw (AppException)
{
    string     port_name = "Unknown";
    int        speed = 20;
    xmlNodePtr cnode = node->children;
    char      *dummy;
    int        vtime = DEFAULT_VTIME;
    
    InputValidator validator;
    validator.addRequiredInput("baud_rate");

    while (cnode) {
        if ( ! xmlStrcasecmp ( cnode->name, (const xmlChar *)"port_name") ) {
            port_name = xmlNodeGetNormContent(cnode);
        }
        else if ( ! xmlStrcasecmp ( cnode->name, (const xmlChar *)"baud_rate") ) {
            speed = strtol (xmlNodeGetNormContent (cnode), &dummy, 10);
            if (speed > 0) {
                validator.setInputStatusOk("baud_rate");
            }
        }
        else if ( ! xmlStrcasecmp ( cnode->name, (const xmlChar *)"vtime") ) {
            vtime = strtol (xmlNodeGetNormContent (cnode), &dummy, 10);
        }
        cnode = cnode->next;
    }

    if (validator.validateInputs() == false) {
        throw AppException(__FILE__, __LINE__, 
                "Incomplete input for establishing serial connection");
    }

    dataSource__.reset(new SerialConn (port_name, speed, vtime));
    return;
}

/**
 * Function to load various data output options by parsing the configuration file.
 *
 * @param node: Pointer to the <DATA> node in the XML configuration file.
 */
void CommInpCfg :: loadDataOutputConfig (const xmlNodePtr node)
        throw (AppException)
{
    TableOpt   tbl_opt;
    char      *properties, *dummy;
    xmlNodePtr cnode = node->children;
   
    InputValidator validator;
    validator.addRequiredInput("collect_table");
    validator.addRequiredInput("table");

    while (cnode) {
        if ( ! xmlStrcasecmp ( cnode->name, (const xmlChar *)"working_path") ) {
            dataOpt__.WorkingPath = xmlNodeGetNormContent (cnode);
        }
        else if ( !xmlStrcasecmp(cnode->name, 
                    (const xmlChar *)"collect_table") ) {
            validator.setInputStatusOk("collect_table");
            xmlNodePtr tnode = cnode->children;
            while (tnode) {
                if (!xmlStrcasecmp(tnode->name, (const xmlChar *)"table")){
                    tbl_opt.TableName = xmlNodeGetNormContent (tnode);
                    validator.setInputStatusOk("table");
                    properties = (char *)xmlGetProp (tnode, 
                            (const xmlChar*)"sample_int_secs");
                    if (properties == NULL) {
                        tbl_opt.SampleInt = -1;
                    }
                    else {
                        tbl_opt.SampleInt = strtol(properties, &dummy, 10);
                    }

                    properties = (char *)xmlGetProp (tnode, 
                            (const xmlChar*)"file_span_secs");
                    if (properties == NULL) {
                        tbl_opt.TableSpan = 3600;
                    }
                    else {
                        tbl_opt.TableSpan = strtol(properties, &dummy, 10);
                        if (tbl_opt.TableSpan <= 0) {
                            tbl_opt.TableSpan = 3600;
                        }
                    }
                    dataOpt__.Tables.push_back (tbl_opt);
                } 
                tnode = tnode->next;
            }
        }
        cnode = cnode->next;
    }

    if (validator.validateInputs() == false) {
        throw AppException(__FILE__, __LINE__, 
                "Incomplete input for data table names");
    }
}

/**
 * Function to load the PakBus address information of the target device from 
 * the input configuration file.
 *
 * @param node: Pointer to the <PAKBUS> node in the XML configuration file.
 */
void CommInpCfg :: loadPakbusConfig (const xmlNodePtr node)
        throw (AppException)
{
    char      *dummy;
    xmlNodePtr cnode = node->children;

    InputValidator validator;
    validator.addRequiredInput("dst_pakbus_id");
    validator.addRequiredInput("dst_node_pakbus_id");
    validator.addRequiredInput("security_code");

    while (cnode) {
        if(!xmlStrcasecmp(cnode->name, (const xmlChar *)"dst_pakbus_id")) {
            pbAddr__.PakBusID = (int) strtol(xmlNodeGetNormContent (cnode), 
                        &dummy, 10);
            validator.setInputStatusOk("dst_pakbus_id");
        }
        else if(!xmlStrcasecmp (cnode->name, 
                     (const xmlChar *)"dst_node_pakbus_id") ) {
            pbAddr__.NodePakBusID = 
                    (int) strtol(xmlNodeGetNormContent(cnode), &dummy, 10);
            validator.setInputStatusOk("dst_node_pakbus_id");
        }
        else if(!xmlStrcasecmp(cnode->name, 
                    (const xmlChar *)"security_code") ) {
            pbAddr__.SecurityCode = 
                    (uint2)strtol(xmlNodeGetNormContent(cnode),&dummy, 10);
            validator.setInputStatusOk("security_code");
        }
        cnode = cnode->next;
    }
    if (validator.validateInputs() == false) {
        throw AppException(__FILE__, __LINE__, 
                "Incomplete input about PakBus configuration");
    }
    return;
}

/**
 * Function to parse the application configuration file (XML).
 * This calls other functions in turn to load various types of information 
 * as device address, communication settings, data output options etc.
 *
 * @param filename: Complete path of the configuration file.
 * @return 0 on success. If the file couldn't be opened or the XML file wasn't
 * well-formed 1 is returned.
 */
void CommInpCfg :: loadConfig (const char *filename) 
        throw (AppException)
{
    xmlDocPtr  CollectionCfg;
    xmlNodePtr node;
    xmlChar   *properties;
    char      *xmlData;

    InputValidator validator;
    validator.addRequiredInput("DATA");
    validator.addRequiredInput("PAKBUS");
    validator.addRequiredInput("CONNECTION");

    string msg("Parsing config file : ");
    msg += filename;
    Category::getInstance("CommInpCfg")
             .debug(msg);

    if ( (CollectionCfg = xmlParseFile (filename)) == NULL) {
        throw AppException (__FILE__, __LINE__, "XML file isn't well-formed");
    }

    xmlNode *root = xmlDocGetRootElement (CollectionCfg);

    if ( !root || !root->name || 
            xmlStrcasecmp (root->name, (const xmlChar *)"collection") != 0 ) {
        xmlFreeDoc (CollectionCfg);
        throw AppException (__FILE__, __LINE__, "No root element in XML file");
    }
    
    xmlData = (char *)xmlGetProp (root, (const xmlChar*)"logger");    
    dataOpt__.LoggerType = (xmlData != NULL) ? xmlData : "N/A";
    xmlData = (char *)xmlGetProp (root, (const xmlChar*)"station_name"); 
    dataOpt__.StationName = (xmlData != NULL) ? xmlData : "N/A";

    for (node = root->children; node != NULL; node = node->next) {
        if (node->type != XML_ELEMENT_NODE) {
            continue;
        }
        if ( ! xmlStrcasecmp (node->name, (const xmlChar *)"DATA") ) {
            loadDataOutputConfig (node);
            validator.setInputStatusOk("DATA");
        }
        else if (!xmlStrcasecmp (node->name, (const xmlChar *)"DEBUG") ) {
            if (strstr(xmlNodeGetNormContent(node), "TRUE")) {
                Category::getRoot().setPriority(Priority::DEBUG);
            }
        }
        else if( !xmlStrcasecmp (node->name, (const xmlChar *)"PAKBUS") ) {
            loadPakbusConfig (node);
            validator.setInputStatusOk("PAKBUS");
        }
        else if(!xmlStrcasecmp(node->name, (const xmlChar *)"CONNECTION")) {
            properties = xmlGetProp (node, (const xmlChar*)"type");
            if (! xmlStrcasecmp (properties, (const xmlChar*)"serial")) {
                // connType__ = "serial";
                loadSerialConfig (node);
                validator.setInputStatusOk("CONNECTION");
            }
        }
    }

    xmlFreeDoc (CollectionCfg);
    xmlCleanupParser ();

    if (validator.validateInputs() == false) {
        throw AppException(__FILE__, __LINE__, "Incomplete config file");
    }
    return; 
}

auto_ptr<DataSource> CommInpCfg :: getDataSource(const string& connectionString) 
        throw (AppException)
{
    if ( (NULL == dataSource__.get()) && (connectionString.size() == 0) ) {
        throw AppException (__FILE__, __LINE__, 
                "No connection information is provided in config file/command line");
    }

    DataSource* dataSourcePtr = dataSource__.get(); 
    if (dataSourcePtr) {
        DataSource::decorate(dataSourcePtr, connectionString);
    }
    else {
        dataSource__.reset(DataSource::createDataSource(connectionString));
    }
    return dataSource__;
}

CommInpCfg :: CommInpCfg()
{
    // Set up logging with stdout as destination
    Category& rootLogCategory = Category::getRoot();
    rootLogCategory.setPriority(Priority::INFO);
    rootLogCategory.removeAllAppenders();

    Appender* appender = new FileAppender("_", dup(fileno(stdout)));
    PatternLayout* patternLayout = new PatternLayout();
    patternLayout->setConversionPattern("[%d{%Y:%m:%d %H:%M:%S.%I}] %p %c %x: %m%n");
    Layout* layout = dynamic_cast<Layout*> (patternLayout);
    appender->setLayout(layout);

    rootLogCategory.addAppender(appender);
}

/** 
 * Destructor for the CommInpCfg class.
 * This frees the connection object used to establish communication with the device.
 */
CommInpCfg :: ~CommInpCfg ()
{
}

void CommInpCfg :: dirSetup () throw (AppException)
{
  string dir(dataOpt__.WorkingPath);
  string errMsg;

  if (setup_dir(dir)) {
      errMsg.append("Failed to setup").append(dir);
      throw AppException(__FILE__, __LINE__, errMsg.c_str());
  }

  dir += "/.working";
  if (setup_dir(dir)) {
      errMsg.append("Failed to setup").append(dir);
      throw AppException(__FILE__, __LINE__, errMsg.c_str());
  }
  return;
}

int CommInpCfg :: redirectLog ()
{
    time_t      curr_t;
    struct tm  *ptm;
    char        log_file_name[32];

    time (&curr_t);
    ptm = gmtime (&curr_t);
    strftime (log_file_name, 32, "%Y%m%d_%H%M%S.log", ptm);

    string AppLogFile(dataOpt__.WorkingPath);
    AppLogFile.append("/")
              .append(log_file_name);
    
    Category& rootLogCategory = Category::getRoot();
    rootLogCategory.removeAllAppenders();

    Appender* appender = new FileAppender("Log", AppLogFile);
    PatternLayout* patternLayout = new PatternLayout();
    patternLayout->setConversionPattern("[%d{%Y:%m:%d %H:%M:%S.%I}] %p %c %x: %m%n");
    Layout* layout = dynamic_cast<Layout*> (patternLayout);
    appender->setLayout(layout);

    cout << "Redirecting logging from stdout to : " << AppLogFile << endl;
    rootLogCategory.addAppender(appender);

    return 0;
}
