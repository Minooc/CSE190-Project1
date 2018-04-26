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
	delete[] bufDescTable;
	delete[] bufPool;
	delete hashTable;
}

void BufMgr::advanceClock()
{
	// Move clock hand
	clockHand++;
	clockHand = clockHand % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	bool frameFound = false;

	// frameLeft is used to check if there's any frame to allocate.
	// Initially set to number of buffers, this value will be decreased when any pinned frame is found.
	// If every frames are pinned, frameLeft will become 0 which means there's no more frame to allocate.
	int frameLeft = numBufs;

	while(!frameFound){
		// advance clock hand
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

					// When no frame is available, throw exception
					if (frameLeft == 0) throw BufferExceededException();
					frameLeft--;

					continue;
				}
			}
			else {
				/* When refbit is 1 */

				// clear refbit and go again
				bufDescTable[clockHand].refbit = 0;
				continue;
			}
		}
		else {
			/* When valid is not set */

			frameFound = true;
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

		/* When page found on buffer pool */

		bufDescTable[frame].refbit = true;	
		bufDescTable[frame].pinCnt ++;

		page = &bufPool[frame];

	}
	catch (const HashNotFoundException& e) {

		/* When page is not on buffer pool */

		FrameId newFrame;
		Page newPage;

		// Allocate buffer frame
		allocBuf(newFrame);

		// Read page from disk into buffer pool
		newPage = file->readPage(pageNo);
		bufPool[newFrame] = newPage;
		bufDescTable[newFrame].valid = 1;

		// Insert the page into the hashtable
		hashTable->insert(file, pageNo, newFrame);

		// Invoke Set() to set it properly
		bufDescTable[newFrame].Set(file, pageNo);

		page = &bufPool[newFrame];

	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	// find appropriate frame containing (file, pageNo)
	for (FrameId i=0; i < numBufs; i++) {
		if (bufDescTable[i].file == file && bufDescTable[i].pageNo == pageNo) {

			// If pin count is already 0, throw error
			if (bufDescTable[i].pinCnt == 0) throw PageNotPinnedException(file->filename(), pageNo, i);

			// Decrement pin count
			bufDescTable[i].pinCnt --;	

			// set the dirty bit
			if (dirty == true) bufDescTable[i].dirty = true;
		}
	}
}

void BufMgr::flushFile(const File* file) 
{
	for (FrameId i=0; i < numBufs; i++){
		if(bufDescTable[i].file == file){
				
			// Create copy of buffer desc instance associated with the file
			BufDesc desc = bufDescTable[i];			
	
			// Check if the frame is pinned
			if(desc.pinCnt != 0) throw PagePinnedException(file->filename(),desc.pageNo,desc.frameNo);

			// Check if the frame is valid
			if(!desc.valid) throw BadBufferException(desc.frameNo,desc.dirty,desc.valid,desc.refbit);
		
			// Check if the frame is dirty before flushing
			if(desc.dirty)	desc.file->writePage(bufPool[i]);
		}
	}	
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	// Allocate new page in the file
	FrameId currFrame; 
	Page newPage = file->allocatePage();
	
	// Allocate the new page in the buffer pool
	allocBuf(currFrame);
	bufPool[currFrame] = newPage;
	bufDescTable[currFrame].valid = 1;
		
	// Set the pageNo and page ptr reference with the new page
	pageNo = bufPool[currFrame].page_number();
	page = &bufPool[currFrame];
	
	// Set new file and pageNo to the frame
	bufDescTable[currFrame].Set(file,pageNo);
	hashTable->insert(file,pageNo,currFrame);
	
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{

	// Remove the page from the file
	file->deletePage(PageNo);
	
	// Remove the associated frame in the bufferPool
	for(uint32_t i = 0; i < numBufs; i++){
		if(bufDescTable[i].file == file && bufDescTable[i].pageNo == PageNo
			&& bufDescTable[i].pinCnt == 0){
			hashTable->remove(file, PageNo);
			// Reset buffer frame
			bufDescTable[i].Clear(); 
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
