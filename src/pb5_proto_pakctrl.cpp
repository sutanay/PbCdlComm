/** 
 * @file pb5_proto_pakctrl.cpp
 * Implements the PakCtrl protocol layer for handshaking and metadata management.
 */

/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: pb5_proto_pakctrl.cpp,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:55:19 $
 *   $State: Exp $
 *****************************************************/

#include <iostream>
#include <fstream>
#include <stdio.h>
#include <unistd.h>
#include <string>
#include "pb5.h"
#include "utils.h"
#include "log4cpp/Category.hh"
using namespace std;
using namespace log4cpp;

/////////////////////////////////////////////////////////////////
// class PakCtrlObj - this class implements various transactions
// supported by the PktCtrl protocol. Refer to pakbus5_proto.h for
// the class declaration.
/////////////////////////////////////////////////////////////////

PakCtrlObj :: PakCtrlObj() : PakBusMsg()
{
    HiProtoCode__ = 0x00;
}


/**
 * Execute a "HelloTransaction" prior to sending a command to a PakBus device.
 */
int PakCtrlObj :: HelloTransaction() throw (PakBusException)
{
    Packet pack;    
    int    stat;
    byte   hop_metric = 0x01;
    bool   dev_replied = false;
    int    sleep_secs = 0;
    byte   hop_metric_response = 0;

    MsgType__    = 0x09;         // Message type
    MsgBodyLen__ = 4;
    MsgBody__[0] = 0x00;         // Source is not router
    MsgBody__[2] = 0x00;         // Link verification interval
    MsgBody__[3] = 0x3c;         // LSB of link verification
                               // interval
    while (hop_metric < 0x06) 
    {
        MsgBody__[1] = hop_metric; 
        GenTranNbr();
        byte tran_id = GenTranNbr();
        
        try {
            SendPBPacket();
            switch (hop_metric) 
            {
                case 0x01 : sleep_secs = 1; 
                            break;
                case 0x02 : sleep_secs = 5; 
                            break;
                case 0x03 : sleep_secs = 10; 
                            break;
                case 0x04 : sleep_secs = 20; 
                            break;
                case 0x05 : sleep_secs = 60; 
                            break;
            }
            sleep (sleep_secs);
          
            pbuf__->readFromDevice();
        }
        catch (CommException& ce) {
            Category::getInstance("PakCtrl")
                     .error("Communication error during HelloTransaction");
            throw;
        }

        while (packetQueue__->size()) {
            pack = packetQueue__->front();
            stat = ParsePakBusPacket (pack, 0x89, tran_id);
            if (stat) {
                PacketErr ("Hello Transaction", pack, stat);
            }
            else {
	        hop_metric_response = (byte)*(pack.begPacket + 12);
                dev_replied = true;
            }
            packetQueue__->pop_front ();
        }

        if (dev_replied) {
            break;
        }
        else {
            hop_metric++;
        }
    }

    if (dev_replied) {
        if ((hop_metric_response < 1) || (hop_metric_response > 5)) {
            sleep_secs = 0;
        }
        else {
            switch (hop_metric_response) 
            {
                case 0x01 : sleep_secs = 1; 
                            break;
                case 0x02 : sleep_secs = 5; 
                            break;
                case 0x03 : sleep_secs = 10; 
                            break;
                case 0x04 : sleep_secs = 20; 
                            break;
                case 0x05 : sleep_secs = 60; 
                            break;
            }
        }
        Category::getInstance("PakCtrl")
                 .debug("Hello Transaction successful");
        return sleep_secs;
    }
    else {
        Category::getInstance("PakCtrl")
                 .debug("Hello Transaction failed");
        throw PakBusException (__FILE__, __LINE__, "Hello Transaction failed");
    }
}

/**
 * Send a "Bye" message before closing connection with the PakBus device.
 */
byte PakCtrlObj :: Bye()
{
  MsgType__ = 0x0d;
  GenTranNbr();
  MsgBodyLen__ = 0;

  ExpMoreCode__ = 0x00;
  LinkState__   = 0x0b;

  try {
      SendPBPacket();
  }
  catch (CommException& ce) {
      Category::getInstance("PakCtrl")
               .error("Communication error while sending bye message");
  }
  return TranNbr__;
}

/* 
 // The following code is useful for configuring the device. Even though
 // they are not used for data collection, I'm leaving them inside.
 //
byte PakCtrlObj :: HelloRequest ()
{
    DstPhyAddr  = 0x0fff;
    ExpMoreCode = 3;
    DstNodeId   = 0x0fff;
    HopCnt      = 0;
    MsgType__     = 0x0e;
    TranNbr     = 0;
    MsgBodyLen__  = 0;
    SendPBPacket();
    return TranNbr;
} 

void PakCtrlObj :: GetSetting (uint2 setting_id)
{
    int stat;
    MsgType__  = 0x0f;
    GenTranNbr();
    MsgBodyLen__ = 4;
    SetSecurityCodeInMsgBody();
    PBSerialize (MsgBody+2, setting_id, 2);
    byte tran_id = TranNbr;
    SendPBPacket();

    pbuf__->readFromDevice();

    while (packetQueue__->size()) {
        Packet pack = packetQueue__->front();
        stat = ParsePakBusPacket (pack, 0x8f, tran_id);
        if (stat) {
            PacketErr ("DevConfig Get Setting Transaction", pack, stat);
        }
        else {
            byte val = (byte)PBDeserialize ((byte *)pack.begPacket + 9, 4);
        }
        packetQueue__->pop_front ();
    }
    return;
}

int PakCtrlObj :: SetSetting (uint2 setting_id, string val)
{
    int stat = generic_set_setting (setting_id, val.length(), (byte *)val.c_str());
    return stat;
}

int PakCtrlObj :: SetSetting (uint2 setting_id, uint2 val)
{
    byte tmp[2];
    PBSerialize (tmp, val, 2);
    int stat = generic_set_setting (setting_id, 2, tmp);
    return stat;
}

int PakCtrlObj :: SetSetting (uint2 setting_id, uint4 val)
{
    byte tmp[4];
    PBSerialize (tmp, val, 4);
    int stat = generic_set_setting (setting_id, 4, tmp);
    return stat;
}

int PakCtrlObj :: generic_set_setting (uint2 setting_id, uint2 setting_len, byte *val)
{
    int stat;
    byte cmd_stat;

    MsgType__  = 0x10;
    GenTranNbr();
    MsgBodyLen__ = 6 + setting_len;

    SetSecurityCodeInMsgBody();
    PBSerialize (MsgBody+2, setting_id, 2);
    PBSerialize (MsgBody+4, setting_len, 2);
    memcpy (MsgBody+6, val, setting_len);
    byte tran_id = TranNbr;
    SendPBPacket();

    pbuf__->readFromDevice();

    while (packetQueue__->size()) {
        Packet pack = packetQueue__->front();
        stat = ParsePakBusPacket (pack, 0x90, tran_id);
        if (stat) {
            PacketErr ("DevConfig Set Setting Transaction", pack, stat);
        }
        else {
            cmd_stat = (byte)(*(pack.begPacket + 11) |
                            *(pack.begPacket + 14));
        }
        packetQueue__->pop_front ();
    }

    if (cmd_stat == 0x01){
        // Commit the changes
        cmd_stat = devconfig_ctrl_transaction (0x01);
    }

    if (cmd_stat == 0x01) {
        return SUCCESS;
    }
    else {
        return FAILURE;
    }
}

int PakCtrlObj :: devconfig_ctrl_transaction (byte action)
{
    int stat;
    byte cmd_stat;

    MsgType__  = 0x13;
    GenTranNbr();
    MsgBodyLen__ = 3;

    SetSecurityCodeInMsgBody();
    MsgBody__[2] = action;
    byte tran_id = TranNbr;
    SendPBPacket();

    pbuf__->readFromDevice();

    while (packetQueue__->size()) {
        Packet pack = packetQueue__->front();
        stat = ParsePakBusPacket (pack, 0x93, tran_id);
        if (stat) {
            PacketErr ("DevConfig Control Transaction", pack, stat);
        }
        else {
            cmd_stat = (byte)(*(pack.begPacket + 11));
        }
        packetQueue__->pop_front ();
    }
    return cmd_stat;
} 

*/
