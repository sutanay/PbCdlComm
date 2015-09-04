/**
 * @file serial_comm.h
 * Contains functions for opening and closing a serial
 * port.
 */

 /*************************************************
 * COPYRIGHT (C) 2000 Battelle Memorial Institute *
 * All Rights Reserved.                           *
 **************************************************
 * Header file for Serial Communication utilities *
 **************************************************
 * Author : Sutanay Choudhury                     *
 * Tel. No. (509)375-3978                         *
 * Email :  sutanay.choudhury@pnl.gov             *
 **************************************************/
 /* RCS INFORMATION:
  *  $RCSfile  : serial_comm.c,v$
  *  $Revision : 1.2 $
  *  $Author : choudhury$
  *  $Locker : choudhury$
  *  $Date : 1/10/2005$
  *  $State: Exp $
  *  $Name:  $
  **************************************************/


#ifndef SERIAL_COMM_H
#define SERIAL_COMM_H

#define SUCCESS 0
#define FAILURE 1

int canon_read(int fd, char *buf, int nbytes);
int OpenComConfig(const char *StrCOMPort, long baudRate, int parity, 
        int dataBits, int stopBits, int vtime);
int CloseCom(int COMfd);
int test_connection(int fd);

#endif
