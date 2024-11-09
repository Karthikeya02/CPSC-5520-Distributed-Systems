import sys
import socket
import pickle
import random
import time as t
from datetime import datetime, timedelta
import threading
from socketserver import ThreadingTCPServer, BaseRequestHandler

# Client State
group_members: dict = {}  # Dictionary to store group members
higher_priority_members: dict = {}  # Members with higher priority
current_leader = None  # Current leader of the group
election_in_progress = False  # Flag to indicate if an election is in progress
failed = False  # Flag to simulate failure

class PeerHandler(BaseRequestHandler):
    """Handle incoming messages from peers."""

    def handle(self):
        global election_in_progress, current_leader, group_members

        try:
            print(f"\nSTARTING WORK for pid {IDENTITY} on {self.client_address} ")
            print(f"BEGIN {self.server.server_address}, {IDENTITY}")
            message_name, message_data = pickle.loads(self.request.recv(1024))
            print(f"Receiving {message_data} from {threading.get_ident()}")

            if message_name == 'BEGIN':
                print(f"Members: {group_members}. Starting an election at startup.")
                start_election()

            elif message_name == 'ELECTION':
                group_members.update(message_data)  # Update known members
                self.request.sendall(pickle.dumps('OK'))

                if not election_in_progress:
                    start_election()

            elif message_name == 'COORDINATOR':
                current_leader = message_data
                election_in_progress = False
                print(f"New leader elected: {current_leader}")

            elif message_name == 'PROBE':
                self.request.sendall(pickle.dumps('OK'))

        except Exception as e:
            print(f"Error handling peer message: {e}")

def gcd_communication() -> dict:
    """
    Communicate with the GCD to register and get initial peer list.

    Returns:
        dict: A dictionary containing the initial list of group members.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((GCDHOST, GCDPORT))
        print(f"BEGIN ({GCDHOST, GCDPORT}) ({IDENTITY}) ({LISTEN_HOST, LISTEN_PORT})")
        print(f"Sending BEGIN ({IDENTITY}) ({LISTEN_HOST, LISTEN_PORT})")
        s.sendall(pickle.dumps(('BEGIN', (IDENTITY, (LISTEN_HOST, LISTEN_PORT)))))
        print(f"Receiving: ({IDENTITY}: {LISTEN_HOST, LISTEN_PORT})")
        data = pickle.loads(s.recv(1024))
        print(f"Members: ({IDENTITY}: {data})")
        return data

def start_election() -> None:
    """
    Initiate an election.
    """
    global election_in_progress, current_leader, higher_priority_members

    election_in_progress = True
    higher_priority_members = {}  # Reset higher_priority_members
    print(f"Starting election with ID: {IDENTITY}")
    higher_priority_members = {k: v for k, v in group_members.items() if k > IDENTITY}

    # If no higher priority members, declare self as the winner
    if not higher_priority_members:
        declare_victory()
    else:
        # Send ELECTION messages to all higher priority members
        responses = []
        for addr in higher_priority_members.values():
            response = send_message(addr, ('ELECTION', group_members))
            if response == 'OK':
                responses.append(response)

        # If no one responds within 2 seconds, declare self as the winner
        if not responses:
            declare_victory()

def declare_victory() -> None:
    """
    Declare self as the new leader.
    """
    global current_leader, election_in_progress
    current_leader = IDENTITY
    election_in_progress = False
    print(f"Victory by {current_leader}, no other bullies bigger than me.")

    # Send COORDINATOR message to all members
    for ids, addr in group_members.items():
        send_message(addr, ('COORDINATOR', current_leader))

def send_message(address: tuple, message: tuple) -> str:
    """
    Send a pickled message to the given address.

    Args:
        address (tuple): The address to send the message to.
        message (tuple): The message to be sent.

    Returns:
        str: The response from the recipient.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(address)
            s.sendall(pickle.dumps(message))
            print(f"Sending {message} to {address} ({threading.get_ident()})")
            return pickle.loads(s.recv(1024))
    except Exception as e:
        return f"Error in sending message as {e}."

def probe_leader() -> None:
    """
    Periodically probe the leader to check if they are alive.
    """
    while True:
        if current_leader and current_leader != IDENTITY:
            t.sleep(random.uniform(0.5, 3))  # Random interval between probes
            response = send_message(list(group_members.keys())[list(group_members.values()).index(current_leader)],('PROBE', None))
            if response != 'OK':
                print("Leader failed! Initiating election...")
                start_election()

def feign_failure() -> None:
    """
    Occasionally pretend to fail and recover.
    """
    global failed
    while True:
        t.sleep(random.uniform(0, 10))
        failed = True
        print("Simulating failure...")
        t.sleep(random.uniform(1, 4))
        failed = False
        print("Recovered from failure. Initiating election...")
        start_election()

if __name__ == '__main__':

    if len(sys.argv) != 5:
        print("Usage: python lab2.py <hostname> <port> <days_to_birthday> <su_id>")
        sys.exit(1)

    # Command-line arguments: GCS Host, GCD Port, Days to Birthday, SU_ID
    GCDHOST = sys.argv[1]
    GCDPORT = int(sys.argv[2])
    DAYS_NEXT_BIRTHDAY = int(sys.argv[3])
    SU_ID = int(sys.argv[4])

    IDENTITY = (DAYS_NEXT_BIRTHDAY, SU_ID)
    LISTEN_HOST = 'localhost'
    LISTEN_PORT = random.randint(10000, 60000)

    next_birthday = datetime.now() + timedelta(days=DAYS_NEXT_BIRTHDAY)
    print(f"Next Birthday: {next_birthday}")
    print(f"SeattleU ID: {SU_ID}")

    # Start the listening server
    server = ThreadingTCPServer((LISTEN_HOST, LISTEN_PORT), PeerHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"Server Loop Running in Thread: {threading.current_thread().name}")

    # Register with GCD and get initial list of members
    group_members = gcd_communication()

    start_election()

    # Keep the main thread alive
    try:
        while True:
            t.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        server.shutdown()
