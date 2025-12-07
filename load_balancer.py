import socket
import threading
import itertools
import sys
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

# The Round Robin index (simple counter)
RR_INDEX = 0

# Lock for thread-safe access to the RR_INDEX and SERVER_STATE
STATE_LOCK = threading.Lock()

def get_next_server():
    """Retrieves the next HEALTHY backend server using the Round Robin algorithm."""
    global RR_INDEX

    with STATE_LOCK:
        start_index = RR_INDEX
        num_servers = len(SERVER_STATE)

        while True:
            server = SERVER_STATE[RR_INDEX]
            
            # Move the index for the next request
            RR_INDEX = (RR_INDEX + 1) % num_servers

            if server['healthy']:
                return (server['host'], server['port'])

            # If we've circled back to the starting point, all servers are down
            if RR_INDEX == start_index:
                # Raise an exception to be caught in proxy_handler
                raise Exception("No healthy servers available.")

def health_checker():
    """Periodically checks the health of all backend servers."""
    while True:
        time.sleep(5)

        with STATE_LOCK:
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


def proxy_handler(client_sock, client_addr):
    """Handles an incoming client connection by proxying to a backend server or returning 503."""
    
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

    try:
        # Try to get a healthy server
        backend_host, backend_port = get_next_server()
        print(f"[{client_addr[0]}:{client_addr[1]}] -> Routing to {backend_host}:{backend_port}")

        # 1. Connect to the selected backend server
        backend_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend_sock.connect((backend_host, backend_port))

        # 2. Receive the request from the client (the initial HTTP request line)
        request_data = client_sock.recv(4096).decode('utf-8')
        if not request_data:
            return

        request_data = request_data.replace('Connection: keep-alive', 'Connection: close')

        # 3. Forward the client's request to the backend server
        backend_sock.sendall(request_data.encode('utf-8'))

        # 4. Receive the response from the backend server
        while True:
            # We assume a fixed buffer size for simplicity. In a real scenario,
            # you'd need to parse headers for Content-Length or Chunked encoding.
            part = backend_sock.recv(4096)
            if not part:
                break

            # 5. Forward the backend's response back to the client
            client_sock.sendall(part)

    except Exception as e:
        error_message = str(e)
        if "No healthy servers available." in error_message:
            print(f"[{client_addr[0]}:{client_addr[1]}] -> ERROR: All servers down. Returning 503.")
            client_sock.sendall(SERVICE_UNAVAILABLE_RESPONSE)
        else:
            print(f"[{client_addr[0]}:{client_addr[1]}] -> Socket error or other failure: {e}")
            client_sock.sendall(SERVICE_UNAVAILABLE_RESPONSE) # Still return 503 on unexpected error
    finally:
        # 6. Close the connection
        try:
            client_sock.close()
            if 'backend_sock' in locals() and backend_sock:
                backend_sock.close()
            print(f"[{client_addr[0]}:{client_addr[1]}] -> Connection closed.")
        except Exception as e:
            pass

def start_load_balancer():
    """Starts the load balancer listener."""
    # Create the load balancer socket
    lb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Allow the socket to be reused immediately after closing
    lb_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        lb_socket.bind(('', LISTEN_PORT))
        lb_socket.listen(5)
        print(f"Load Balancer running on port {LISTEN_PORT}. Backends: {len(SERVER_STATE)} defined.")

        # --- Start the Health Check Thread ---
        health_thread = threading.Thread(target=health_checker, daemon=True)
        health_thread.start()
        print("Health check thread started.")
        # -------------------------------------

        while True:
            # Accept an incoming client connection
            client_sock, client_addr = lb_socket.accept()
            print(f"Incoming connection from {client_addr[0]}:{client_addr[1]}")

            # Start a new thread to handle the proxying for this client
            thread = threading.Thread(
                target=proxy_handler,
                args=(client_sock, client_addr)
            )
            thread.start()

    except KeyboardInterrupt:
        print("\nStopping Load Balancer...")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        lb_socket.close()
        sys.exit(0)

if __name__ == '__main__':
    start_load_balancer()


