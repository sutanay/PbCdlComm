/**
 * @file serial_comm.c
 * Contains functions for opening and closing a serial
 * port.
 */

/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: serial_comm.c,v $
 *   $Revision: 1.1.1.1 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/05/21 01:09:52 $
 *   $State: Exp $
 *****************************************************/

#include <stdio.h>
#include <termios.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include "serial_comm.h"

int canon_read(int dev_fd, char *buf, int nbytes) 
{
    int nread = 0;
    char *read_ptr = buf;
    while(1) {
       if ((nread = read (dev_fd, read_ptr, 1)) > 0){
           nread++;
           if ((*read_ptr == '\r') || (*read_ptr == '\n') ||
                   (nread == nbytes) || (*read_ptr == '\0')) {
               break;
           }
           read_ptr++;
       }
       else {
           break;
       } 
    } 
    *read_ptr = '\0';
    return nread;
}

/**
 * Function to open a serial device and configure the connection 
 * based on the input parameters.
 * 
 * @param portname:  Complete path of the serial device.
 * @param baudRate:  Baudrate specified as integer.
 * @param parity:    Number of parity bits.
 * @param data_bits: Number of data bits.
 * @param stop_bits: Number of stop bits.
 * @param vtime:     Multiples of 0.1 seconds specified until which 
 *                   a read() will block before returning.
 * @return Returns the file descriptor on success, else -1
 */
int OpenComConfig(const char StrCOMPort[], long baudRate,
        int parity, int dataBits, int stopBits, int vtime)
{
    int COMfd;
    struct termios COMPort;

    if((COMfd = open(StrCOMPort, O_RDWR | O_NOCTTY | O_NONBLOCK)) == -1){
        printf("\tError opening serial port : %s\n", strerror(errno));
        return(-1);
    }
    fcntl(COMfd, F_SETFL, 0); 
    if(tcgetattr(COMfd, &COMPort) < 0){
        return(-1);
    }

    switch(baudRate){
        case 9600 :  cfsetispeed(&COMPort, B9600);
                     cfsetospeed(&COMPort, B9600);
                     break;
        case 19200 : cfsetispeed(&COMPort, B19200);
                     cfsetospeed(&COMPort, B19200);
                     break;
        case 38400 : cfsetispeed(&COMPort, B38400);
                     cfsetospeed(&COMPort, B38400);
                     break;
        case 57600 : cfsetispeed(&COMPort, B57600);
                     cfsetospeed(&COMPort, B57600);
                     break;
        case 115200: cfsetispeed(&COMPort, B115200);
                     cfsetospeed(&COMPort, B115200);
                     break;
        default    : cfsetispeed(&COMPort, B9600);
                     cfsetospeed(&COMPort, B9600);
    }

    switch(parity){
        case 0:  COMPort.c_cflag &= ~(PARENB | CSIZE);
                 COMPort.c_cflag |= CS8;
                 break;
        case 1 : COMPort.c_cflag |= (PARENB | CS7);
                 COMPort.c_cflag &= ~(PARODD | CSIZE);
                 break;
        case 2:  COMPort.c_cflag |= (PARENB | PARODD | CS7);
                 COMPort.c_cflag &= ~CSIZE;
                 break;
        default: COMPort.c_cflag &= ~(PARENB | CSIZE);
                 COMPort.c_cflag |= CS8;
    }

    switch(dataBits){
        case 5 : COMPort.c_cflag |= CS5;
                 break;
        case 6 : COMPort.c_cflag |= CS6;
                 break;
        case 7 : COMPort.c_cflag |= CS7;
                 break;
        case 8 : COMPort.c_cflag |= CS8;
                 break;
        default: COMPort.c_cflag |= CS8;
    }

    switch(stopBits){
        case 1  : COMPort.c_cflag &= ~CSTOPB;
                  break;
        case 2  : COMPort.c_cflag |= CSTOPB;
                  break;
        default : COMPort.c_cflag &= ~CSTOPB;
    }

    /* |enable receiver| don't change owener of COMPort| */
    /* COMPort.c_cflag |= (CREAD | CLOCAL); */
    COMPort.c_cflag |= (CREAD | CLOCAL | HUPCL);

    /**********************************************
     * Following block to set up for raw operation
     **********************************************/

    /* |disable canonical input | disable signals | no echo| */
    COMPort.c_lflag &= ~(ISIG | ECHO | ECHOE | ICANON); 

    /* Disable input processing; no parity checking or marking;
     * no signals from break conditions; do not convert line feeds
     * or carriage returns and do not map upper case to lower case */

    COMPort.c_iflag &= ~(INPCK | PARMRK | BRKINT | INLCR | ICRNL);
    COMPort.c_iflag &= ~(INPCK | ISTRIP | IGNBRK);  
    COMPort.c_iflag &= ~(IXON | IXOFF | IXANY);
   
    /* don't postprocess output = raw output */
    COMPort.c_oflag &= ~OPOST;
    COMPort.c_oflag &= ~ONLCR; /* Don't convert line feeds */
    
    /* Enable input and hangup line (drop DTR) on last close */

    COMPort.c_cc[VTIME] = vtime;  
    COMPort.c_cc[VMIN]  = 0;

    if(tcsetattr(COMfd, TCSANOW, &COMPort) != 0){
        printf("\tError configuring serial port : %s\n", strerror(errno));
        return(-1);
    }
    else{
        return(COMfd);
    }
}

/**
 * Function to close the specified serial port.
 *
 * @param  COMfd: File descriptor for the serial port
 * @return Returns 0 on success, else the error number
 */
int CloseCom(int COMfd){
    int result;
    tcflush (COMfd, TCSAFLUSH);
    result = close(COMfd);
    if (result < 0){
        return(errno);
    }
    else{
        return(SUCCESS);
    }
}

/**
 * This function checks if the serial port has a device connected to it.
 * This is done by checking the DSR (Data Set Ready) "Modem" status bit 
 * associated with the serial port.
 *
 * @param fd:     File descriptor for the serial port
 * @return  SUCCESS | FAILURE
 */
int TestConnection(int fd)
{
    int status;
    
    ioctl(fd, TIOCMGET, &status);
    /* First enable the Request To Send (RTS) bit */
    status |= TIOCM_RTS;
    ioctl(fd, TIOCMSET, &status);
    ioctl(fd, TIOCMGET, &status);

    /* Now check if the Clear To Send (CTS) bit is enabled */

    if((status & TIOCM_CTS) == 0){
        return 1;
    }
    else{
        return 0;
    }
}
