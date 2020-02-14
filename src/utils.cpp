/** 
 * @file utils.cpp
 * Implements various utility functions.
 */

/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: utils.cpp,v $
 *   $Revision: 1.3 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/09/02 20:40:11 $
 *   $State: Exp $
 *****************************************************/

#include <cstring>
#include <iostream>
#include <iterator>
#include <map>
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <log4cpp/Category.hh>
#include <libxml2/libxml/parser.h>
#include <libxml2/libxml/tree.h>
// #define __USE_GNU
#include <ucontext.h>
#include <execinfo.h>
#include "utils.h"

using namespace std;
using namespace log4cpp;

/**
 * FUNCTION
 *    open_lockfile(char *StrLockFile, char *process_name)
 *
 * SYNOPSIS
 *    This function checks for the existence of a lockfile to determine if
 *    the daemon is already running. Otherwise, it creates a lockfile
 *    and signals the calling function to start the daemon process.
 *
 * INPUT PARAMETERS
 *    char *StrLockFile : Name of the lockfile, i.e. /var/tmp/ttySI0_IRT.lck
 *    char *process_name : The name of the locking process is written out
 *
 * RETURN VALUE
 *    0 on SUCCESS | 1 on FAILURE
 *
 */

int open_lockfile(const char *lock_file, const char *process_name){
    char cmd[256];
    time_t t_stamp;
    pid_t process_id = getpid();
     
    ofstream ofs_lock;
    ofs_lock.open (lock_file, ofstream::out);
    if ( !ofs_lock.is_open () ) {
        return 1;
    }
    time(&t_stamp);
    ofs_lock << "Opened by : " << process_name << endl;
    ofs_lock << "PID of locking process : " << process_id << endl;
    ofs_lock << "File created on " << ctime(&t_stamp) << endl;
    ofs_lock.close();

    sprintf(cmd, "chmod 744 %s", lock_file);
    system(cmd);

    return 0;
}

/****************************************************************************
 * FUNCTION
 *    is_running(char *StrLockFile)
 *
 * SYNOPSIS
 *    This function checks for the existence of a lockfile to determine if
 *    the daemon is already running.
 *
 * INPUT PARAMETERS
 *    char *StrLockFile : Name of the lockfile, i.e. /var/tmp/ttySI0_IRT.lck
 *
 * RETURN VALUE
 *    1 if the daemon is running, else 0
 *
 * **************************************************************************/

int is_running(const char *StrLockFile){
    FILE *fp;
    char buf[40];
    int stat;
    pid_t pid = -1;

    if((fp = fopen(StrLockFile, "r")) != NULL){
        /* Lock file exists. Read the lock file to obtain the PID
         * of the process that created it. And check if the process
         * is still running. Existence of the lock file should be
         * enough to indicate that but this will ensure that the
         * process didn't die without removing the lockfile.*/
        fgets(buf, 40, fp);
        /* Now that the first line has been read, read the PID from
         * the second line */
        fgets(buf, 40, fp);
        fclose(fp);
        sscanf(buf, "PID of locking process : %d\n", &pid);
        if((stat = kill(pid, 0)) == 0){
            return pid;
        }
        else{
            printf("The last run exited without removing lock file\n");
            printf("Removing outdated lock file\n");
            unlink(StrLockFile);
            return(0);
        }
    }
    else{
        return(0);
    }
}

/****************************************************************************
 * 
 * FUNCTION
 *    setup_dir (char *dirpath)
 *
 * SYNOPSIS
 *    This function creates the specified directory if it doesn't exist yet.
 *
 * RETURN VALUE
 *    Returns 1 on error, 0 on success
 *
 * **************************************************************************/

int setup_dir (const string& dirpath)
{
    int stat;
    stat = mkdir (dirpath.c_str(), S_IRWXU | S_IRWXG);
    if (stat == 0) {
        Category::getInstance("Utils").info("Created : " + dirpath);
        return 0;
    }
    else {
       if (errno != EEXIST) {
           Category::getInstance("Utils").info("Failed to create : " + dirpath);
           return 1;
       }
       else {
           return 0;
       }
    }
}

/**
 * Function to insert the current timestamp in the low-level log.
 */

char* get_timestamp() 
{
    static time_t     log_t;
    static struct tm* log_ptm;
    static char       log_timestamp[32];

    time (&log_t);
    log_ptm = gmtime (&log_t);
    strftime (log_timestamp, 32, "[%Y:%m:%d %H:%M:%S]: ", log_ptm);
    return log_timestamp;
}

/**
 * Construtor for the AppException class that defines the syntax to use for
 * throwing the exception.
 *
 * @param file: Typically this would be __FILE__
 * @param line: Typically this would be __LINE__
 * @param msg:  A character string describing the error message
 */
AppException :: AppException (const char* file, size_t line, const char* msg) 
: fileName__ (file), lineNum__(line), errMsg__ (msg)
{
}

/**
 * This function returns a character string describing the error message and 
 * the location in the source file where the exception was thrown.
 */
const char* AppException :: what() const throw()
{
    stringstream errstrm;
    errstrm << errMsg__ << " (" << fileName__ << "[" << lineNum__ << "])";
    return errstrm.str().c_str();
}

/**
 * Returns a normalized version of the text content of a XML node.
 * This function strips all leading and trailing whitespaces and newlines.
 *
 * @param node: Pointer to the xmlNode
 * @return char* :  A pointer to the substring representing the node's 
 *              content.
 */
char* xmlNodeGetNormContent(xmlNodePtr node) 
{
    string content;
    xmlChar *value;
    int len;
    int start = 0;
    int end;

    value = xmlNodeGetContent(node);
    len = xmlStrlen(value);

    if (!len) {
        return "";
    }
    else {
        while (start < len) {
            if ((value[start] == ' ') || (value[start] == '\n')) {
                start++;
            }
            else {
                break;
            }
        }

        if (start == len) {
            return "";
        }
        end = len-1;

        while (end >= start) {
            if ((value[end] == ' ') || (value[end] == '\n')) {
                end--;
            }
            else {
                break;
            }
        }

        return ((char *) xmlStrsub(value, start, end - start + 1));
    }
}

/**
 * Adds the name of a required input parameter to validation list.
 * Upon insertion, the validated state is set to false in the map.
 * @param inputName: Name of the input parameter to validate.
 * @return void.
 */
void InputValidator :: addRequiredInput(char *inputName) 
{
    inputMap[inputName] = false;
}

/**
 * Set input status as Ok in the validation map.
 * Typically this would be set by application code after checking 
 * inputs.
 *
 * @param inputName: Name of the validated input parameter.
 * @return void.
 */ 
void InputValidator :: setInputStatusOk(char *inputName)
{
    // If entry for inputName exists in the Map

    if(inputMap.find(inputName) != inputMap.end()) {
        inputMap[inputName] = true;
    }
}

/**
 * Function to iterate over all required inputs and check their state.
 * The function also prints out name of the input parameter that failed
 * validation check in application code and hence wasn't set through
 * the setInputStatusOk() function. 
 * 
 * @return bool : true if all inputs were validated, false otherwise.
 */
bool InputValidator :: validateInputs() 
{
    bool test_flag = true;
    map<string, bool>::iterator itr;
    bool value;

    for (itr = inputMap.begin(); itr != inputMap.end(); ++itr) {
        value = itr->second;
        if (value == false) {
            cout << "Missing input parameter : " << itr->first << endl;
        }
        test_flag &= value;
    }
    return test_flag;
}

int byte2int (char c) { return (0x000000ff & (unsigned char)c); }


/**
 * Function to print a description of a signal in the log file.
 */
void printSigInfo(int signum)
{
    char msg[64];

    switch (signum) {

        case SIGQUIT:
            strcpy(msg, "Received Signal -> SIGQUIT: Quit (see termio(7I))");
            break;
        case SIGILL:
            strcpy(msg, "Received Signal -> SIGILL: Illegal Instruction");
            break;
        case SIGTRAP:
            strcpy(msg, "Received Signal -> SIGTRAP: Trace or Breakpoint Trap");
            break;
        case SIGABRT:
            strcpy(msg, "Received Signal -> SIGABRT: Abort");
            break;
        case SIGFPE:
            strcpy(msg, "Received Signal -> SIGFPE: Arithmetic Exception");
            break;
        case SIGBUS:
            strcpy(msg, "Received Signal -> SIGBUS: Bus Error");
            break;
        case SIGSEGV:
            strcpy(msg, "Received Signal -> SIGSEGV: Segmentation Fault");
            break;
        case SIGSYS:
            strcpy(msg, "Received Signal -> SIGSYS: Bad System Call");
            break;
        case SIGHUP:
            strcpy(msg, "Received Signal -> SIGHUP: Hangup (see termio(7I))");
            break;
        case SIGINT:
            strcpy(msg, "Received Signal -> SIGINT: Interrupt (see termio(7I))");
            break;
        case SIGPIPE:
            strcpy(msg, "Received Signal -> SIGPIPE: Broken Pipe");
            break;
        case SIGALRM:
            strcpy(msg, "Received Signal -> SIGALRM: Alarm Clock");
            break;
        case SIGTERM:
            strcpy(msg, "Received Signal -> SIGTERM: Terminated");
            break;
        default:
            strcpy(msg, "Received Signal -> Unknown Signal Type");
    }
    Category::getInstance("SignalHandler").error(msg);
    return;
}

static char stackTraceBuffer[3072];

void printStackTrace()
{
    void *trace[16];
    char **func_symbols = (char **)NULL;
    int i, trace_size = 0;
    int msg_size = 0;

    trace_size = backtrace(trace, 10);
    func_symbols = backtrace_symbols(trace, trace_size);

    cout << "------------------------------------------------------------" 
         << endl;
    strcpy(stackTraceBuffer, "Stack Trace :\n");
    strcat(stackTraceBuffer,
           "------------------------------------------------------------");

    for (i = 2; i < trace_size; ++i) {
        msg_size += (strlen(func_symbols[i]) + 5);
        if (msg_size < 2800) {
            strcat(stackTraceBuffer, "\n\t");
            strcat(stackTraceBuffer, func_symbols[i]);
        }
        else {
            strcat(stackTraceBuffer, "\n\t....\n");
            break;
        }
    }
    // strcat(stackTraceBuffer,
    //        "\n------------------------------------------------------------");
    Category::getInstance("SignalHandler").error(stackTraceBuffer);
}

void setSignalHandler(void (*exit_handler)(int signum))
{
    signal(SIGABRT, exit_handler);
    signal(SIGALRM, exit_handler);
    signal(SIGBUS, exit_handler);
    signal(SIGFPE, exit_handler);
    signal(SIGHUP, exit_handler);
    signal(SIGILL, exit_handler);
    signal(SIGINT, exit_handler);
    signal(SIGPIPE, exit_handler);
    signal(SIGQUIT, exit_handler);
    signal(SIGSEGV, exit_handler);
    signal(SIGSYS, exit_handler);
    signal(SIGTERM, exit_handler);
    
    return;
}
