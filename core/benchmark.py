import time
from core.mmap_ring_buffer import MMapRingBuffer

def benchmark_ring_buffer():
    # Initialize a 1MB buffer for intensive benchmarking
    buffer_size = 1024 * 1024  # 1 MB
    ring_buffer = MMapRingBuffer(size=buffer_size)
    
    num_iterations = 50_000
    print(f"Starting MMapRingBuffer benchmark with {num_iterations:,} operations...")

    # --- 1. Benchmark Write Speed ---
    start_time = time.perf_counter_ns()
    
    for i in range(num_iterations):
        # Write mock order ID and value
        success = ring_buffer.write(msg_id=i, value=15000 + (i % 100))
        if not success:
            # If buffer gets full in test, reset pointers for continuous benchmarking
            ring_buffer.head = 0
            ring_buffer.tail = 0
            ring_buffer.write(msg_id=i, value=15000 + (i % 100))
            
    end_time = time.perf_counter_ns()
    total_write_time_ns = end_time - start_time
    
    writes_per_sec = (num_iterations / (total_write_time_ns / 1_000_000_000))
    avg_write_latency_ns = total_write_time_ns / num_iterations

    print(f"\n--- Write Benchmark Results ---")
    print(f"Total Write Time: {total_write_time_ns / 1_000_000:.3f} ms")
    print(f"Throughput: {writes_per_sec:,.2f} writes/sec")
    print(f"Average Write Latency: {avg_write_latency_ns:.2f} ns per operation")

    # Reset pointers to read back what we wrote
    ring_buffer.head = 0
    ring_buffer.tail = 0
    # Refill for read test
    for i in range(num_iterations):
        ring_buffer.write(msg_id=i, value=15000)
    
    ring_buffer.head = (num_iterations * ring_buffer.msg_size) % ring_buffer.size
    ring_buffer.tail = 0

    # --- 2. Benchmark Read Speed ---
    start_time = time.perf_counter_ns()
    
    read_count = 0
    while True:
        msg = ring_buffer.read()
        if msg is None:
            break
        read_count += 1
        
    end_time = time.perf_counter_ns()
    total_read_time_ns = end_time - start_time
    
    reads_per_sec = (read_count / (total_read_time_ns / 1_000_000_000))
    avg_read_latency_ns = total_read_time_ns / read_count if read_count > 0 else 0

    print(f"\n--- Read Benchmark Results ---")
    print(f"Total Read Time: {total_read_time_ns / 1_000_000:.3f} ms")
    print(f"Throughput: {reads_per_sec:,.2f} reads/sec")
    print(f"Average Read Latency: {avg_read_latency_ns:.2f} ns per operation")

    ring_buffer.close()

if __name__ == "__main__":
    benchmark_ring_buffer()
