import asyncio
from mmap_ring_buffer import MMapRingBuffer

# Import Teammate 1's firehose async generator from the firehose package[cite: 7, 8]
from firehose.firehose import firehose

async def consume_firehose_to_mmap(ring_buffer: MMapRingBuffer):
    """
    Consumes orders from Teammate 1's firehose() async generator 
    and writes them into your mmap ring buffer.
    """
    print("Listening to Teammate 1's firehose stream and writing to MMapRingBuffer...")
    
    # Call the firehose async generator with specified rate and duration[cite: 7]
    async for order in firehose(order_rate=500, duration=3):
        # Extract fields based on the generated order structure
        if isinstance(order, dict):
            msg_id = order.get("order_id", 101)
            value = int(order.get("price", 1000))
        else:
            # Fallback if order is returned as an object attribute
            msg_id = getattr(order, "order_id", 101)
            value = int(getattr(order, "price", 1000))
        
        # Write using your mmap ring buffer write method
        success = ring_buffer.write(msg_id, value)
        
        if success:
            print(f"[Buffer Write Success] Order ID: {msg_id}, Value: {value}")
        else:
            print(f"[Buffer Overflow] Dropped Order ID: {msg_id}")

if __name__ == "__main__":
    # Initialize your mmap ring buffer
    buffer = MMapRingBuffer(size=2048)
    
    # Run the async ingestion loop connecting the firehose to the buffer
    asyncio.run(consume_firehose_to_mmap(buffer))
    
    # Read back what was ingested to test
    print("\nReading ingested orders back from mmap buffer:")
    while True:
        msg = buffer.read()
        if msg is None:
            break
        print(f"Unpacked from mmap -> {msg}")
        
    buffer.close()