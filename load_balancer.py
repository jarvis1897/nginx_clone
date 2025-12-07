import socket
import threading
import itertools
import sys

# ----------------------------------------------------
# 1. Configuration
# ----------------------------------------------------
# The list of backend servers (the "upstreams")
BACKEND_SERVERS = [
    ('127.0.0.1', 8081),
    ('127.0.0.1', 8082),
]

LISTEN_PORT = 8000

# The Round Robin iterator
# cycle() makes an iterator that repeats the list indefinitely
server_iterator = itertools.cycle(BACKEND_SERVERS)

# Lock for thread-safe access to the server_iterator
RR_LOCK = threading.Lock()

def get_next_server():
    """Retrieves the next backend server using the Round Robin algorithm."""
    with RR_LOCK:
        # Get the next server from the cyclic iterator
        return next(server_iterator)

def proxy_handler(client_sock, client_addr):
    """Handles an incoming client connection by proxying to a backend server."""
    backend_host, backend_port = get_next_server()
    print(f"[{client_addr[0]}:{client_addr[1]}] -> Routing to {backend_host}:{backend_port}")

    try:
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
        response_data = b""
        while True:
            # We assume a fixed buffer size for simplicity. In a real scenario,
            # you'd need to parse headers for Content-Length or Chunked encoding.
            part = backend_sock.recv(4096)
            if not part:
                break
            response_data += part

            # 5. Forward the backend's response back to the client
            client_sock.sendall(response_data)

    except socket.error as e:
        print(f"Socket error during proxy: {e}")
        # A simple HTTP 503 response could be sent here instead of just closing
    finally:
        # 6. Close the connection
        client_sock.close()
        backend_sock.close()
        print(f"[{client_addr[0]}:{client_addr[1]}] -> Connection closed.")

def start_load_balancer():
    """Starts the load balancer listener."""
    # Create the load balancer socket
    lb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Allow the socket to be reused immediately after closing
    lb_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        lb_socket.bind(('', LISTEN_PORT))
        lb_socket.listen(5)
        print(f"Load Balancer running on port {LISTEN_PORT}. Backends: {BACKEND_SERVERS}")

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


