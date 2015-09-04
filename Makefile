#####################################################
# RCS INFORMATION:
#   $RCSfile: Makefile,v $
#   $Revision: 1.2 $
#   $Author: choudhury $
#   $Locker:  $
#   $Date: 2008/06/10 20:17:29 $
#   $State: Exp $
#####################################################

##
##   Usage:
##
##   make            - make compile&link executable into bin/pbcdl_comm
##   make clean      - remove ./obj/ & ./bin/ files
##   make install    - copy pbcdl_comm executable from $(OUT_DIR), eg: ./bin
##                     to operational bin directory $(OP_BIN_DIR), eg: ../bin/
##

OBJ_DIR  = ./obj
C_SRCS   = $(shell ls *.c)
OBJS     = $(C_SRCS:%.c=$(OBJ_DIR)/%.o)
CPP_SRCS = $(shell ls *.cpp)
OBJS    += $(CPP_SRCS:%.cpp=$(OBJ_DIR)/%.o)

CFLAGS    = -O -g -c -pedantic -Wall `xml2-config --cflags`
#XMLCFLAGS = `xml2-config --cflags`
#IFLAGS    = 
LFLAGS    = -rdynamic 
LFLAGS    = 
XMLLFLAGS = `xml2-config --libs` 

##############################################################################
# Edit to modify output options 
##############################################################################
CC        = g++
OUT_DIR = ./bin
OP_BIN_DIR = /usr/local/bin
BIN_NAME  = pbcdl_comm

##############################################################################
# Edit to add include directories or libraries
##############################################################################
IFLAGS   += -I/home/choudhury/apps/install/Linux-i686/include
LFLAGS   += -L/home/choudhury/apps/install/Linux-i686/lib -llog4cpp -lpthread

##############################################################################

TARGET = $(OUT_DIR)/$(BIN_NAME)

all: $(BIN_NAME)

$(BIN_NAME): $(OBJS)
	@mkdir -p $(OUT_DIR)
	@mkdir -p $(OBJ_DIR)
	$(CC) -o $(OUT_DIR)/$(BIN_NAME) $(OBJS) $(LFLAGS) $(XMLLFLAGS) 

$(OBJ_DIR)/main.o  : main.cpp collection_process.h
	$(CC) -o $(OBJ_DIR)/main.o $(CFLAGS) main.cpp $(IFLAGS) 

$(OBJ_DIR)/pb5_proc.o  : pb5_proc.cpp
	$(CC) -o $(OBJ_DIR)/pb5_proc.o $(CFLAGS) pb5_proc.cpp $(IFLAGS) 

$(OBJ_DIR)/pb5_buf.o  : pb5_buf.cpp pb5_buf.h
	$(CC) -o $(OBJ_DIR)/pb5_buf.o $(CFLAGS) pb5_buf.cpp $(IFLAGS) 

$(OBJ_DIR)/pb5_data.o  : pb5_data.cpp pb5_data.h
	$(CC) -o $(OBJ_DIR)/pb5_data.o $(CFLAGS) pb5_data.cpp $(IFLAGS) 

$(OBJ_DIR)/pb5_data_writer.o  : pb5_data_writer.cpp pb5_data.h
	$(CC) -o $(OBJ_DIR)/pb5_data_writer.o $(CFLAGS) pb5_data_writer.cpp $(IFLAGS) 

$(OBJ_DIR)/pb5_proto_base.o  : pb5_proto_base.cpp pb5_proto.h
	$(CC) -o $(OBJ_DIR)/pb5_proto_base.o $(CFLAGS) pb5_proto_base.cpp $(IFLAGS) 

$(OBJ_DIR)/pb5_proto_bmp.o  : pb5_proto_bmp.cpp pb5_proto.h
	$(CC) -o $(OBJ_DIR)/pb5_proto_bmp.o $(CFLAGS) pb5_proto_bmp.cpp $(IFLAGS) 

$(OBJ_DIR)/pb5_proto_pakctrl.o  : pb5_proto_pakctrl.cpp pb5_proto.h
	$(CC) -o $(OBJ_DIR)/pb5_proto_pakctrl.o $(CFLAGS) pb5_proto_pakctrl.cpp $(IFLAGS) 

$(OBJ_DIR)/init_comm.o  : init_comm.cpp init_comm.h 
	$(CC) -o $(OBJ_DIR)/init_comm.o $(CFLAGS) init_comm.cpp $(IFLAGS)

$(OBJ_DIR)/serial_comm.o  : serial_comm.c serial_comm.h
	@mkdir -p $(OBJ_DIR)
	$(CC) -o $(OBJ_DIR)/serial_comm.o $(CFLAGS) serial_comm.c

$(OBJ_DIR)/utils.o  : utils.cpp utils.h
	$(CC) -o $(OBJ_DIR)/utils.o $(CFLAGS) utils.cpp $(IFLAGS)

clean  : 
	rm -f $(TARGET)
	rm -f $(OBJS)

install:
	@echo "make install: copying $(TARGET) to $(OP_BIN_DIR)"
	@mkdir -p $(OP_BIN_DIR)
	@/bin/cp -p $(TARGET) $(OP_BIN_DIR)


