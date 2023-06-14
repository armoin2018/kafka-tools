import argparse
import json
import re
import socket
import time
import csv
import sys

def test_host(host, port, timeout=3):
    try:
        start_time = time.time()
        with socket.create_connection((host, port), timeout=timeout):
            response_time = time.time() - start_time
            return True, response_time
    except (socket.timeout, ConnectionRefusedError):
        return False, timeout

def calculate_statistics(response_times):
    if len(response_times) > 0:
        min_response = min(response_times)
        max_response = max(response_times)
        avg_response = sum(response_times) / len(response_times)
    else:
        min_response = None
        max_response = None
        avg_response = None
    return min_response, max_response, avg_response

def format_prometheus(results):
    prometheus_output = []
    for result in results:
        host = result['Target Host']
        port = result['Port']
        state = result['State']
        min_response = result['Minimum Response Time']
        max_response = result['Maximum Response Time']
        avg_response = result['Average Response Time']
        
        prometheus_output.append(f'probe_{host}_{port}_state{{}} {state}')
        prometheus_output.append(f'probe_{host}_{port}_min{{}} {min_response}')
        prometheus_output.append(f'probe_{host}_{port}_max{{}} {max_response}')
        prometheus_output.append(f'probe_{host}_{port}_avg{{}} {avg_response}')
    
    return '\n'.join(prometheus_output)

def format_splunk(results):
    splunk_output = []
    for result in results:
        host = result['Source Host']
        target_host = result['Target Host']
        port = result['Port']
        state = result['State']
        min_response = result['Minimum Response Time']
        max_response = result['Maximum Response Time']
        avg_response = result['Average Response Time']
        
        splunk_output.append(f'{host} target_host={target_host} port={port} state={state} min_response={min_response} max_response={max_response} avg_response={avg_response}')
    
    return '\n'.join(splunk_output)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--HostList', type=str, required=True, help='Comma-separated list of hosts to test')
    parser.add_argument('--PortList', type=str, required=True, help='Comma-separated list of ports to test')
    parser.add_argument('--Timeout', type=int, default=3, help='Timeout value in seconds (default: 3)')
    parser.add_argument('--Response', type=str, help='Regular expression pattern for expected response')
    parser.add_argument('--Samples', type=int, default=1, help='Number of samples to take per host-port combination (default: 1)')
    parser.add_argument('--OutputFormat', default='csv', choices=['csv', 'json', 'prometheus', 'splunk'], help='Output format (default: csv)')
    parser.add_argument('--Help', action='store_true', help='Show help message')

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    if args.Help:
        parser.print_help(sys.stderr)
        sys.exit(0)

    host_list = args.HostList.split(',')
    port_list = args.PortList.split(',')
    timeout = args.Timeout
    expected_response = args.Response
    num_samples = args.Samples
    output_format = args.OutputFormat

    results = []

    for host in host_list:
        for port in port_list:
            response_times = []
            up_count = 0
            down_count = 0

            for _ in range(num_samples):
                is_up, response_time = test_host(host, port, timeout)
                if is_up:
                    up_count += 1
                else:
                    down_count += 1

                response_times.append(response_time)

            min_response, max_response, avg_response = calculate_statistics(response_times)

            state = "up" if up_count > down_count else "down"

            if expected_response:
                if re.search(expected_response, str(response_times)):
                    state = "up"
                else:
                    state = "down"

            result = {
                'Source Host': socket.gethostname(),
                'Target Host': host,
                'Port': port,
                'State': state,
                'Minimum Response Time': min_response,
                'Maximum Response Time': max_response,
                'Average Response Time': avg_response
            }

            results.append(result)

    if output_format == 'csv':
        keys = results[0].keys() if results else []
        writer = csv.DictWriter(sys.stdout, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)
    elif output_format == 'json':
        print(json.dumps(results, indent=4))
    elif output_format == 'prometheus':
        print(format_prometheus(results))
    elif output_format == 'splunk':
        print(format_splunk(results))

if __name__ == '__main__':
    main()
