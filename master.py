import rpyc
import uuid
import math
import random
import configparser
import signal
import pickle
import sys
import os
import json


from rpyc.utils.server import ThreadedServer

from rpyc.utils.server import AuthenticationError

BLOCK_SIZE = 100
REPLICATION_FACTOR = 2
MINIONS = {"1": ("127.0.0.1", 8000),
		   "2": ("127.0.0.1", 8001),
		   "3": ("127.0.0.1", 8002)}

class MasterService(rpyc.Service):

	file_block = {}
	block_minion = {}
	minions = MINIONS

	#read metada in files
	with open("file_block.json", "r") as fb:
		file_block = json.load(fb)

	with open("block_minion.json", "r") as fb:
		block_minion = json.load(fb)

	block_size = BLOCK_SIZE
	replication_factor = REPLICATION_FACTOR

	def exposed_delete(self, file):

		mapping = []
		# iterate over all of file's blocks

		try:
			for blk in self.file_block[file]:
				minion_addr = []
				# get all minions that contain that block
				for m_id in self.block_minion[blk]:
					minion_addr.append(self.minions[m_id])
				mapping.append({"block_id": blk, "block_addr": minion_addr})
				self.block_minion.pop(blk)
				
			self.file_block.pop(file)
			
			with open("file_block.json", "w") as fb:
				json.dump(self.file_block, fb)

			with open("block_minion.json", "w") as fb:
				json.dump(self.block_minion, fb)

			return mapping
		except Exception as e:
			print('Failed to find file')


	def exposed_read(self, file):

		mapping = []
		# iterate over all of file's blocks

		try:
			for blk in self.file_block[file]:
				minion_addr = []
				# get all minions that contain that block
				for m_id in self.block_minion[blk]:
					minion_addr.append(self.minions[m_id])

				mapping.append({"block_id": blk, "block_addr": minion_addr})

			return mapping
		except Exception as e:
			print('Failed to find file')

			

	def exposed_write(self, file, size):

		self.file_block[file] = []

		num_blocks = int(math.ceil(float(size) / self.block_size))
		return self.alloc_blocks(file, num_blocks)

	def alloc_blocks(self, file, num_blocks):
		return_blocks = []
		list_minions = []

		#verify which minions are up
		for m in self.minions:
			host, port = self.minions[m]
			try:
				rpyc.connect(host, port=port)
				list_minions.append(m)
			except Exception as e:
				print("Failed to connect to port:", port)
				continue
		
		#end writing if no minion were found
		if (len(list_minions)==0):
			return -1
		
		for i in range(0, num_blocks):
			block_id = str(uuid.uuid1()) # generate a block
			minion_ids = random.sample(     # allocate REPLICATION_FACTOR number of minions
				list_minions, self.replication_factor)
			minion_addr = [self.minions[m] for m in minion_ids]
			self.block_minion[block_id] = minion_ids
			self.file_block[file].append(block_id)

			#store metadata
			with open("file_block.json", "w") as fb:
				json.dump(self.file_block, fb)

			with open("block_minion.json", "w") as fb:
				json.dump(self.block_minion, fb)

			return_blocks.append(
				{"block_id": block_id, "block_addr": minion_addr})

		return return_blocks


if __name__ == "__main__":
	t = ThreadedServer(MasterService(), port=2131, protocol_config={
	'allow_public_attrs': True,
})
	t.start()
