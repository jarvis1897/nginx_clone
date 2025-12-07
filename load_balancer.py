import socket
import asyncio
import itertools
import time

# ----------------------------------------------------
# 1. Configuration
# ----------------------------------------------------
# The list of backend servers (the "upstreams") with their state
SERVER_STATE = [
    {'host': '127.0.0.1', 'port' : 8081, 'healthy': True, 'name': "Server 1"},
    {'host': '127.0.0.1', 'port' : 8082, 'healthy': True, 'name': "Server 2"},
]

LISTEN_PORT = 8000

# Global index and lock
RR_INDEX = 0
STATE_LOCK = asyncio.Lock()

async def get_next_server():
    """Retrieves the next HEALTHY backend server using the Round Robin algorithm."""
    global RR_INDEX
    
    # Use the async lock to ensure only one coroutine modifies RR_INDEX and checks state
    async with STATE_LOCK: 
        start_index = RR_INDEX
        num_servers = len(SERVER_STATE)
        
        while True:
            server = SERVER_STATE[RR_INDEX]
            
            # Move the index for the next request attempt
            RR_INDEX = (RR_INDEX + 1) % num_servers
            
            if server['healthy']:
                return (server['host'], server['port'])
            
            # If we've checked every server and came back to the start, all are down
            if RR_INDEX == start_index:
                raise Exception("No healthy servers available.")

async def health_checker_async():
    """Periodically checks the health of all backend servers."""
    while True:
        await asyncio.sleep(5) # Wait asynchronously for 5 seconds

        # Acquire lock before modifying the shared state
        async with STATE_LOCK:
            for server in SERVER_STATE:
                host = server['host']
                port = server['port']
                is_healthy = False
                
                try:
                    probe_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    probe_sock.settimeout(1)
                    probe_sock.connect((host, port))
                    probe_sock.close()
                    is_healthy = True
                except socket.error:
                    is_healthy = False
                
                # Update the server state
                if server['healthy'] != is_healthy:
                    server['healthy'] = is_healthy
                    status = "UP" if is_healthy else "DOWN"
                    print(f"\n[HEALTH CHECK ALERT] Server {server['name']} ({host}:{port}) is now {status}!")
    
async def async_proxy_handler(client_reader, client_writer):
    """Handles an incoming client connection asynchronously."""
    
    # 503 Service Unavailable HTTP Response
    RESPONSE_BODY = b"Service Unavailable. No healthy backend servers.\n"
    CONTENT_LENGTH = str(len(RESPONSE_BODY)).encode('utf-8')
    
    SERVICE_UNAVAILABLE_RESPONSE = (
        b"HTTP/1.1 503 Service Unavailable\r\n"
        b"Content-Type: text/plain\r\n"
        b"Connection: close\r\n" # Explicitly tell curl we are closing
        b"Content-Length: " + CONTENT_LENGTH + b"\r\n"
        b"\r\n"
        + RESPONSE_BODY
    )

    client_addr = client_writer.get_extra_info('peername')
    print(f"Incoming connection from {client_addr[0]}:{client_addr[1]}")

    try:
        # 1. Get request data from client
        request_data = await client_reader.read(4096)
        if not request_data:
            return
        
        # 2. Select a healthy backend server
        backend_host, backend_port = await get_next_server()
        print(f"[{client_addr[0]}:{client_addr[1]}] -> Routing to {backend_host}:{backend_port}")

        # 3. Connect to the selected backend server asynchronously
        backend_reader, backend_writer = await asyncio.open_connection(backend_host, backend_port)

        # 4. Modify and forward the request
        request_str = request_data.decode('utf-8').replace('Connection: keep-alive', 'Connection: close')
        backend_writer.write(request_str.encode('utf-8'))
        await backend_writer.drain() # Ensure the data is written immediately

        # 5. Shunt response data from backend to client
        while True:
            # Asynchronously read chunks from the backend
            part = await backend_reader.read(4096)
            if not part:
                break

            # Asynchronously write chunks to the client
            client_writer.write(part)
            await client_writer.drain() # Ensure the data is sent immediately

    except Exception as e:
        error_message = str(e)
        if "No healthy servers available." in error_message or "Connection refused" in error_message:
            print(f"[{client_addr[0]}:{client_addr[1]}] -> ERROR: All servers down. Returning 503.")
            client_writer.write(SERVICE_UNAVAILABLE_RESPONSE)
        else:
            print(f"[{client_addr[0]}:{client_addr[1]}] -> Async error: {e}")
            client_writer.write(SERVICE_UNAVAILABLE_RESPONSE)

    finally:
        # 6. Close the connection
        client_writer.close()
        await client_writer.wait_closed()
        if 'backend_writer' in locals():
            backend_writer.close()
            await backend_writer.wait_closed()
        print(f"[{client_addr[0]}:{client_addr[1]}] -> Connection closed.")
        

async def start_load_balancer_async():
    """Starts the load balancer listener and the health checker tasks."""

    # 1. Start the health check as a background task
    health_task = asyncio.create_task(health_checker_async())
    print("Health check task started.")
    
    # 2. Start the main listener server
    server = await asyncio.start_server(
        async_proxy_handler, 
        '0.0.0.0', # Listen on all interfaces
        LISTEN_PORT
    )

    addr = server.sockets[0].getsockname()
    print(f"Async Load Balancer running on port {addr[1]}. Backends: {len(SERVER_STATE)} defined.")

    # 3. Run forever
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    try:
        asyncio.run(start_load_balancer_async())
    except KeyboardInterrupt:
        print("\nStopping Load Balancer...")
    except Exception as e:
        print(f"An error occurred: {e}")




