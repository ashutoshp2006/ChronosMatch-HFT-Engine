import asyncio
from core.mmap_ring_buffer import MMapRingBuffer

# Import Teammate 2's function when available (or mock it for testing)
from core.firehose import firehouse 

async def mock_firehose():
    """Placeholder matching Teammate 2's firehouse async generator function."""
    sample_orders = [
        (201, 15100),
        (202, 15150),
        (203, 15080),
    ]
    for msg_id, val in sample_orders:
        await asyncio.sleep(0.4)
        yield {"msg_id": msg_id, "value": val}

async def consume_firehose_to_mmap(ring_buffer: MMapRingBuffer):
    """
    Consumes from Teammate 2's firehouse() async generator 
    and writes to your mmap ring buffer.
    """
    print("Listening to firehose stream and writing to MMapRingBuffer...")
    
    # Replace mock_firehose() with firehouse() once Teammate 2 merges their code
    async for order in mock_firehose():
        msg_id = order["msg_id"]
        value = order["value"]
        
        # Write using your mmap ring buffer write method
        success = ring_buffer.write(msg_id, value)
        
        if success:
            print(f"[Buffer Write Success] Order ID: {msg_id}, Value: {value}")
        else:
            print(f"[Buffer Overflow] Dropped Order ID: {msg_id}")

if __name__ == "__main__":
    # Initialize your mmap ring buffer (e.g., 1024 bytes)
    buffer = MMapRingBuffer(size=1024)
    
    # Run the async ingestion loop
    asyncio.run(consume_firehose_to_mmap(buffer))
    
    # Read back what was ingested to test
    print("\nReading ingested orders back from mmap buffer:")
    while True:
        msg = buffer.read()
        if msg is None:
            break
        print(f"Unpacked from mmap -> {msg}")
        
    buffer.close()
