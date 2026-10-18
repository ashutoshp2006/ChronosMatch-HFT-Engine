import mmap
import struct
import os

class MMapRingBuffer:
    def __init__(self, size: int = 1024):
        self.size = size
        # Format string: 
        # 'I' = unsigned int (4 bytes) for message type/id
        # 'q' = long long (8 bytes) for integer value/timestamp
        self.struct_format = 'Iq'
        self.msg_size = struct.calcsize(self.struct_format) # 12 bytes per message
        
        # Create an anonymous memory-mapped file buffer
        self.mmap_file = mmap.mmap(-1, size)
        
        # Head (write pointer) and Tail (read pointer) indices
        self.head = 0
        self.tail = 0
        self.is_full = False

    def write(self, msg_id: int, value: int) -> bool:
        """
        Packs integers using struct and writes to the mmap buffer.
        Optimized with circular overwrite logic when the buffer is full.
        """
        packed_data = struct.pack(self.struct_format, msg_id, value)
        
        next_head = (self.head + self.msg_size) % self.size
        
        # Check if buffer is full (head has caught up to tail)
        if next_head == self.tail:
            # Circular Overwrite: Forcefully advance tail to drop/overwrite the oldest unread packet
            self.tail = (self.tail + self.msg_size) % self.size
            
        # Write data into memory-mapped region at the head pointer
        self.mmap_file.seek(self.head)
        self.mmap_file.write(packed_data)
        self.head = next_head
        return True

    def read(self):
        """Reads from mmap buffer and unpacks bytes back into integers."""
        if self.head == self.tail:
            return None  # Buffer empty
            
        self.mmap_file.seek(self.tail)
        packed_data = self.mmap_file.read(self.msg_size)
        
        # Unpack binary data back to original integers
        msg_id, value = struct.unpack(self.struct_format, packed_data)
        
        self.tail = (self.tail + self.msg_size) % self.size
        return {"msg_id": msg_id, "value": value}

    def close(self):
        self.mmap_file.close()