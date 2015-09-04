/**
 * @file utils.h
 * Collection of general utlities.
 */
/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: utils.h,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:54:46 $
 *   $State: Exp $
 *****************************************************/

#ifndef UTILS_H
#define UTILS_H
#include <map>
#include <string>
#include <exception>
#include <libxml2/libxml/tree.h>
using namespace std;

/**
 * @fn Function
 * Function to open a lockfile for a particular proces. 
 */
int  open_lockfile (const char *lock_file, const char *process_name);
int  is_running (const char *StrLockFile);
int  setup_dir (const string& dirpath);
char* get_timestamp ();

/**
 * A class derived from std::exception for error handling.
 */
class AppException : public exception {
    public :
        explicit AppException (const char* file, size_t line, const char* msg);
        const char* what() const throw(); 
        ~AppException() throw() {};

    private :
        string fileName__;
        int    lineNum__;
        string errMsg__;
}; 

/**
 * A no-implementation extension of the AppException class to represent 
 * data parsing related failures.
 */
class ParseException : public AppException {
public:
    explicit ParseException(const char* file, size_t line, const char* msg) :
        AppException(file, line, msg) {}
};

/** 
 * A no-implementation extension of the AppException class to represent
 * exceptions occuring during I/O related actions.
 */
class IOException : public AppException {
public:
    explicit IOException(const char* file, size_t line, const char* msg) :
        AppException(file, line, msg) {}
};

class CommException : public IOException {
public:
    explicit CommException(const char* file, size_t line, const char* msg) :
        IOException(file, line, msg) {}
};

class StorageException : public IOException {
public:
    explicit StorageException(const char* file, size_t line, const char* msg) :
        IOException(file, line, msg) {}
};


/** 
 * A no-implementation extension of the AppException class to represent
 * exceptions related to the PakBus protocol
 */
class PakBusException : public AppException {
public:
    explicit PakBusException(const char* file, size_t line, const char* msg) :
        AppException(file, line, msg) {}
};

class InvalidTDFException : public AppException {
public:
    explicit InvalidTDFException(const char* file, size_t line, const char* msg) :
        AppException(file, line, msg) {}
};


char* xmlNodeGetNormContent(xmlNodePtr node);

// TODO Replace this with a XML Schema validation
/** 
 * A quick and dirty implementation of a means for validating configuration file
 * entries to look for required inputs. Ideally this should be taken care of by
 * DTD or schema validation.
 */
class InputValidator {
    public :
        InputValidator() {};
        ~InputValidator() {};
        void addRequiredInput(char *inputName);
        void setInputStatusOk(char *name);
        bool validateInputs();
   
    private :
        map<string, bool> inputMap;
};

int byte2int(char c);
void setSignalHandler(void (*exit_handler)(int signum));
void printSigInfo(int signum);
void printStackTrace();

#endif
