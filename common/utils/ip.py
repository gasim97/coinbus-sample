def ip_address() -> str:
    import socket
    return socket.gethostbyname(socket.gethostname())