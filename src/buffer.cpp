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
	frame = -1; //Default frameId for not found
	FrameId initial = clockHand;
	bool frameFound = false;
	while(!frameFound){
		advanceClock();
		
		//Check if the clockHand already did a full rotation
		if(initial == clockHand){
			break;
		}

		//Check if the frame is valid
		if(bufDescTable[clockHand].valid){

			//Check the refbit of the frame
			if(!bufDescTable[clockHand].refbit){

				//Check the pin count of the frame
				if(bufDescTable[clockHand].pinCnt == 0){
					frameFound = true;
					frame = bufDescTable[clockHand].frameNo;
				
					//Check if the frame is dirty	
					if(bufDescTable[clockHand].dirty == 1){
						//Write the frame(page) to the disk
						bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
					}
					//Reset the frame	
					bufDescTable[clockHand].Clear();
					break;
				}
			}

			else {
				//clear refbit
				bufDescTable[clockHand].refbit = 0;
			}
		}

		else {
			frameFound = true;
			bufDescTable[clockHand].valid = 1;
			frame = bufDescTable[clockHand].frameNo;
			break;
		}
	}

}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{	
	FrameId frame;
	try {

		hashTable->lookup(file, pageNo, frame);

		// page found on buffer pool
		
		bufDescTable[frame].refbit = true;	
		bufDescTable[frame].pinCnt ++;
		page = &bufPool[frame];

	}
	catch (const HashNotFoundException& e) {
		// page is not on buffer pool

		allocBuf(frame);
		file->readPage(pageNo);
		hashTable->insert(file, pageNo, frame);
		bufDescTable[frame].Set(file, pageNo);

		page = &bufPool[frame];

	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	for (FrameId i=0; i < sizeof(bufDescTable); i++) {
		if (bufDescTable[i].file == file && bufDescTable[i].pageNo == pageNo) {

			/************************************* first parameter? *******************/
			if (bufDescTable[i].pinCnt == 0) throw PageNotPinnedException("", pageNo, i);

			bufDescTable[i].pinCnt --;	
			if (dirty == true) bufDescTable[i].dirty = true;
		}
	}
}

void BufMgr::flushFile(const File* file) 
{
	

}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId currFrame;
	Page newPage = file->allocatePage();
	
	//Allocate the new page in the buffer pool.
	allocBuf(currFrame);
	
	if(currFrame != -1){
		//Assign bufPool with the newly created page
		bufPool[currFrame] = newPage;
		
		//Set the pageNo and page ptr reference with the new page
		pageNo = bufPool[currFrame].page_number();
		page = &bufPool[currFrame];
	
		//Set new file and pageNo to the frame and hashTable
		bufDescTable[currFrame].Set(file,pageNo);
		hashTable->insert(file,pageNo,currFrame);
	}
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{

	//Remove the page from the file
	file->deletePage(PageNo);
	
	//Remove the associated frame in the bufferPool
	for(uint32_t i = 0; i < numBufs; i++){
		if(bufDescTable[i].file == file && bufDescTable[i].pageNo == PageNo
			&&bufDescTable[i].pinCnt == 0){
			//Remove from hashTable	
			hashTable->remove(file, PageNo);
			
			//Clear Frame
			bufDescTable[i].Clear();
			
			//Remove the page from buffer pool or no need?
			break;
		}
	}
	
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
