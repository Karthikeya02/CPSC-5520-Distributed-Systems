"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Karthikeya Panangipalli
:Version: f24-01
Simple Client
"""

import socket
import sys
import pickle


def connect_to_gcd(hostname, port):
    """
    Connects to the GCD server and retrieves the list of group members.

    :param hostname: The hostname of the GCD server.
    :param port: The port number of the GCD server.
    :return: The list of group members.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd_socket:
            gcd_socket.connect((hostname, port))
            print(f"BEGIN ('{hostname}', {port})")
            gcd_socket.sendall(pickle.dumps('BEGIN'))
            data = gcd_socket.recv(4096)
            group_members = pickle.loads(data)
            return group_members
    except socket.error as e:
        print(f"failed to connect to GCD: {e}")
        sys.exit(1)


def connect_to_group_member(member):
    """
    Connects to a group member and sends a 'HELLO' message.

    :param member: The group member to connect to.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as member_socket:
            member_socket.settimeout(1.5)
            member_socket.connect((member['host'], member['port']))
            print(f"HELLO to {member}")
            member_socket.sendall(pickle.dumps('HELLO'))
            response_data = member_socket.recv(4096)
            response = pickle.loads(response_data)
            print(response)
    except socket.timeout:
        print(f"Timeout occurred for group member {member['host']}:{member['port']}")
    except socket.error as e:
        print(f"HELLO to {member}")
        print(f"failed to connect: {e}")


def main():
    """
    The main function of the program.
    """
    if len(sys.argv) != 3:
        print("Usage: python lab1.py <hostname> <port>")
        sys.exit(1)
    gcd_hostname = sys.argv[1]
    gcd_port = int(sys.argv[2])
    group_members = connect_to_gcd(gcd_hostname, gcd_port)

    if group_members:
        for member in group_members:
            connect_to_group_member(member)
    else:
        print("No group members found or, an error occurred while getting group members.")


if __name__ == '__main__':
    main()