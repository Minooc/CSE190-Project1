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

  frameLeft = numBufs;
}


BufMgr::~BufMgr() {
	printf("destructor called\n");
	delete[] bufDescTable;
	delete[] bufPool;
	delete hashTable;
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

		//Check if the frame is valid
		if(bufDescTable[clockHand].valid){

			//Check the refbit of the frame
			if(!bufDescTable[clockHand].refbit){

				//Check the pin count of the frame
				if(bufDescTable[clockHand].pinCnt == 0){
					frameFound = true;
					frame = bufDescTable[clockHand].frameNo;
					hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
				
					//Check if the frame is dirty	
					if(bufDescTable[clockHand].dirty == 1){
						//Write the frame(page) to the disk
						bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
					}
					//Reset the frame	
					bufDescTable[clockHand].Clear();
					break;
				}
				else {
					//No more frame available
					frameLeft--;
					if (frameLeft == 0) throw BufferExceededException();
					continue;
				}
			}
			else {
				//clear refbit
				bufDescTable[clockHand].refbit = 0;
				continue;
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

		FrameId newFrame;
		Page newPage;

		allocBuf(newFrame);

		// read page from disk into buffer pool
		newPage = file->readPage(pageNo);
		bufPool[newFrame] = newPage;

		hashTable->insert(file, pageNo, newFrame);
		bufDescTable[newFrame].Set(file, pageNo);

		page = &bufPool[newFrame];

	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	for (FrameId i=0; i < numBufs; i++) {
		if (bufDescTable[i].file == file && bufDescTable[i].pageNo == pageNo) {

			if (bufDescTable[i].pinCnt == 0) throw PageNotPinnedException(file->filename(), pageNo, i);

			bufDescTable[i].pinCnt --;	
			if (bufDescTable[i].pinCnt == 0) frameLeft ++;
			if (dirty == true) bufDescTable[i].dirty = true;
		}
	}
}

void BufMgr::flushFile(const File* file) 
{
	for (FrameId i=0; i < numBufs; i++){
		if(bufDescTable[i].file == file){
			BufDesc desc = bufDescTable[i];
			
			//Check if the frame is pinned or valid before getting flushed
			if(desc.pinCnt != 0) throw PagePinnedException(file->filename(),desc.pageNo,desc.frameNo);
			if(!desc.valid) throw BadBufferException(desc.frameNo,desc.dirty,desc.valid,desc.refbit);
			if(desc.dirty)	desc.file->writePage(bufPool[i]);
		}
		
	}	

}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId currFrame;
	Page newPage = file->allocatePage();
	
	//Allocate the new page in the buffer pool.
	allocBuf(currFrame);
	bufPool[currFrame] = newPage;
		
	//Set the pageNo and page ptr reference with the new page
	pageNo = bufPool[currFrame].page_number();
	page = &bufPool[currFrame];
	
	//Set new file and pageNo to the frame
	bufDescTable[currFrame].Set(file,pageNo);
	hashTable->insert(file,pageNo,currFrame);
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{

	//Remove the page from the file
	file->deletePage(PageNo);
	
	//Remove the associated frame in the bufferPool
	for(uint32_t i = 0; i < numBufs; i++){
		if(bufDescTable[i].file == file && bufDescTable[i].pageNo == PageNo
			&& bufDescTable[i].pinCnt == 0){
			hashTable->remove(file, PageNo);
			bufDescTable[i].Clear(); //rest buffer frame
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
