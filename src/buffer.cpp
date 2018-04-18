/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

void BufMgr::advanceClock()
{
	clockHand++;
	clockHand = clockHand % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	bool frameFound = false;
	while(!frameFound){
		advanceClock();

		if(bufDescTable[clockHand].valid){

			if(!bufDescTable[clockHand].refbit){

				if(bufDescTable[clockHand].pinCnt == 0){
					frameFound = true;
					frame = bufDescTable[clockHand].frameNo;
				
					if(bufDescTable[clockHand].dirty == 0){
						//clear the frame in the bufPool
						bufDescTable[clockHand].Clear();
					}
					else{
						//flush page to disk, then clear the frame
						bufDescTable[clockHand].file->writePage(bufPool[clockHand]);				
						bufDescTable[clockHand].Clear();
					}

				}

				else{
					//advance clock pointer
					advanceClock();
				}
			}

			else {
				//clear refbit
				bufDescTable[clockHand].refbit = 0;
			}
		}

		else {
			//Call Set() on the frame??
			frameFound = true;
			bufDescTable[clockHand].valid = 1;
			frame = bufDescTable[clockHand].frameNo;
			
		}
	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	Page newPage = file->allocatePage();
	//Allocate the new page in the buffer pool.
	//allocBuf();
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
