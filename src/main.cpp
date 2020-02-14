/**
 * @file main.cpp
 * Contains the entry point to the pbcdl_comm application.
 */

#include <memory>
#include <typeinfo>
#include <iostream>
#include "collection_process.h"
#include "utils.h"
#include <log4cpp/Category.hh>
using namespace std;
using namespace log4cpp;

void atExit(int signum)
{
    static int caught(0);
    caught++;
    if (caught > 1) {
        Category::getInstance("SignalHandler")
                 .warn("Exiting on multiple signal reception");
        printSigInfo(signum);
        exit(1);
    }
    printSigInfo(signum);
    printStackTrace();
    DataCollectionProcessManager::getInstance().cleanup();
    exit(1);
}

int main (int argc, char *argv[])
{
    int stat;

    setSignalHandler(atExit);
    stat = DataCollectionProcessManager::getInstance()
            .run(DataCollectionProcessManager::PB5, argc, argv);

    /*  
    if (0 == stat) {
        cout << "STATUS : Data collection succcessful." << endl;
    } else {
        cout << "STATUS : Data collection failed." << endl;
    }
    */
    return stat;
}
