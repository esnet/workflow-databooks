import csv

def get_rtt_values():
    rtt_fields_map = {'site': 'Site',
                      'values_tcp_rtt_min': 'Min', 'values_tcp_rtt_max': 'Max',
                      'values_tcp_rtt_std': 'StdDev', 'values_tcp_rtt_avg': 'Avg'}

    filename = 'data/rtt.csv'
    display_contents(filename, rtt_fields_map)

def get_metrics():
    metrics_fields_map = {'site': 'Site', 'throughput': 'Tput',
                          'retransmit': 'Rexmit%', 'winscale': 'WinScale'}

    filename = 'data/metrics.csv'
    display_contents(filename, metrics_fields_map)

def display_contents(filename, fields_map):
    with open(filename, 'rb') as f:
        reader = csv.reader(f)
        header = next(reader)
        col_headers = []
        for hname in header:
            col_headers.append(fields_map[hname])
        print('{}'.format('\t'.join(col_headers)))
        print('-------------------------------------')
        for row in reader:
            cols = '\t'.join(row)
            print(cols)
    

def demo():
    print("Demo")

if __name__ == '__main__':
    demo()
