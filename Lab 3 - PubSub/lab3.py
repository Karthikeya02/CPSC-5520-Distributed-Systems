import socket
import struct
import math
import time
from datetime import datetime, timedelta

from fxp_bytes_subscriber import parse_message
from bellman_ford import BellmanFord

MICROS_PER_SECOND = 1_000_000

def get_ip_address():
    """
    Get the local IP address of the machine.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't need to be reachable
        s.connect(('10.255.255.255', 1))
        ip_addr = s.getsockname()[0]
    except Exception:
        ip_addr = '127.0.0.1'
    finally:
        s.close()
    return ip_addr

def subscribe(sock, provider_address, ip_address, port):
    """
    Send subscription request to the forex provider.
    
    Args:
        sock (socket.socket): The UDP socket.
        provider_address (tuple): The address of the forex provider.
        ip_address (str): The local IP address.
        port (int): The local port number.
    """
    ip_bytes = socket.inet_aton(ip_address)
    port_bytes = struct.pack('!H', port)
    subscription_request = ip_bytes + port_bytes
    sock.sendto(subscription_request, provider_address)
    print(f'Sent subscription request to {provider_address}')

def process_message(data, latest_timestamps, quotes):
    """
    Process the received message and update quotes.
    
    Args:
        data (bytes): The received message data.
        latest_timestamps (dict): Dictionary to track the latest timestamps for each market.
        quotes (dict): Dictionary to store the quotes.
    """
    quotes_list = parse_message(data)
    for quote in quotes_list:
        currency1 = quote['currency1']
        currency2 = quote['currency2']
        rate = quote['rate']
        timestamp = quote['timestamp']
        market = (currency1, currency2)
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # Check if the message is out-of-sequence
        if market in latest_timestamps:
            if timestamp <= latest_timestamps[market]:
                print(f'{timestamp_str} {currency1} {currency2} {rate}')
                print('Ignoring out-of-sequence message')
                continue
        
        # Update the latest timestamp and quotes
        latest_timestamps[market] = timestamp
        expiration = timestamp + timedelta(seconds=1.5)
        quotes[market] = {'rate': rate, 'timestamp': timestamp, 'expiration': expiration}
        print(f'{timestamp_str} {currency1} {currency2} {rate}')

def remove_expired_quotes(quotes):
    """
    Remove expired quotes from the quotes dictionary.
    
    Args:
        quotes (dict): Dictionary to store the quotes.
    """
    current_time = datetime.utcnow()
    expired_markets = []
    
    # Identify expired quotes
    for market in quotes:
        expiration = quotes[market]['expiration']
        if current_time > expiration:
            expired_markets.append(market)
    
    # Remove expired quotes
    for market in expired_markets:
        del quotes[market]
        print(f'Removing stale quote for {market}')

def build_graph(quotes):
    """
    Build the graph from the current quotes.
    
    Args:
        quotes (dict): Dictionary to store the quotes.
    
    Returns:
        BellmanFord: The graph built from the quotes.
        dict: Dictionary of edge rates.
    """
    graph = BellmanFord()
    edge_rates = {}
    
    for market in quotes:
        currency1, currency2 = market
        rate = quotes[market]['rate']
        
        # Add edges to the graph
        weight1 = -math.log(rate)
        graph.add_edge(currency1, currency2, weight1)
        edge_rates[(currency1, currency2)] = rate
        
        weight2 = math.log(rate)
        graph.add_edge(currency2, currency1, weight2)
        edge_rates[(currency2, currency1)] = 1 / rate
    
    return graph, edge_rates

def run_bellman_ford(graph):
    """
    Run Bellman-Ford algorithm on the graph to detect negative cycles.
    
    Args:
        graph (BellmanFord): The graph built from the quotes.
    
    Returns:
        list: The negative cycle if found, otherwise None.
    """
    vertices = graph.vertices
    if not vertices:
        return None
    
    start_vertex = next(iter(vertices))
    distance, predecessor, negative_cycle_edge = graph.shortest_paths(start_vertex)
    
    if negative_cycle_edge:
        cycle = []
        u, v = negative_cycle_edge
        cycle.append(v)
        current = u
        while current not in cycle:
            cycle.append(current)
            current = predecessor[current]
        cycle.append(current)
        cycle.reverse()
        return cycle
    else:
        return None

def report_arbitrage(cycle, edge_rates):
    """
    Report the arbitrage opportunity.
    
    Args:
        cycle (list): The negative cycle detected by Bellman-Ford algorithm.
        edge_rates (dict): Dictionary of edge rates.
    """
    log = []
    log.append("ARBITRAGE:")
    amount = 100.0  # Starting with USD 100
    curr_from = "USD"
    log.append(f'\tstart with {curr_from} {amount}')
    
    for i in range(len(cycle) - 1):
        curr_to = cycle[i + 1]
        rate = edge_rates.get((curr_from, curr_to))
        if rate is None:
            log.append(f'Rate from {curr_from} to {curr_to} not found.')
            return log
        amount *= rate
        log.append(f'\texchange {curr_from} for {curr_to} at {rate} --> {curr_to} {amount}')
        curr_from = curr_to

    # Convert back to USD if the last currency is not USD
    if curr_from != "USD":
        curr_to = "USD"
        rate = edge_rates.get((curr_from, curr_to))
        if rate is None:
            log.append(f'No exchange rate available to convert {curr_from} back to USD.')
            return log
        amount *= rate
        log.append(f'\texchange {curr_from} for {curr_to} at {rate} --> {curr_to} {amount}')

    # Extract the final amount from the log
    final_amount_str = log[-2].split()[-1]
    final_amount = float(final_amount_str)

    # Print the log only if Arbitrage is successful
    if final_amount > 100:
        print("\n".join(log))


def main():
    """
    Main function to run the forex arbitrage detection.
    """
    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', 0))
    ip_address = get_ip_address()
    port = sock.getsockname()[1]

    print(f'Subscribing with IP {ip_address} and port {port}')

    # Forex provider address (adjust as needed)
    forex_provider_address = ('localhost', 50403)
    subscribe(sock, forex_provider_address, ip_address, port)

    start_time = time.time()
    latest_timestamps = {}
    quotes = {}

    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time > 10 * 60:
            print('Subscription period over. Exiting.')
            break

        sock.settimeout(10)
        try:
            data, address = sock.recvfrom(4096)
            process_message(data, latest_timestamps, quotes)
            remove_expired_quotes(quotes)
            graph, edge_rates = build_graph(quotes)
            cycle = run_bellman_ford(graph)
            if cycle:
                report_arbitrage(cycle, edge_rates)
        except socket.timeout:
            print('No messages received for 10 seconds. Exiting.')
            break

if __name__ == '__main__':
    main()