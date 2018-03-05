import getpass
import subprocess
import elasticsearch # requires elasticsearch (pip install elasticsearch)
import pandas as pd
from pandas.io.json import read_json
from dns import resolver,reversename # needs dnspython package. pip install dnspython


from requests.utils import DEFAULT_CA_BUNDLE_PATH

def create_tunnel (proxy_host=None,cluster="cheshire.nersc.gov",user=None,local_port=1443,cluster_port=443):
    if proxy_host == None:
        proxy_host = getpass.getpass("proxy host:")
    if user == None:
        user = getpass.getuser()
    args = ["ssh", user + '@' + proxy_host,'-N','-L',str(local_port) + ':' + cluster + ':' + str(cluster_port)]
    p = subprocess.Popen(args=args)
    return p

def kill_tunnel(tunnel):
    tunnel.kill()
    tunnel.wait()
    tunnel.poll()



def esdb_connect(host="cheshire.nersc.gov",port=1443, ca_certs=None, user=None, password=None):
    if ca_certs == None:
        ca_certs = DEFAULT_CA_BUNDLE_PATH
    if user == None and password == None:
        if user == None:
            user = getpass.getuser()
        password = getpass.getpass(prompt="password: ")
        
    esdb = elasticsearch.Elasticsearch([{
        'host': host,
        'port': port,
        'use_ssl': True,
        'verify_certs': False, 'ca_certs': ca_certs, 'http_auth': (user, password),
    }])
    return esdb


def import_data (json):
    out = []
    def flatten(j,e,name=''):
        if type(j) is dict:
            for a in j:
                if not a in ('_source','values','meta'):
                    flatten(j[a],e, name + a + '_')
                else:
                    flatten(j[a],e, name)
        else:
            e[name[:-1]] = j
                
    for record in json:
        entry = {}
        out.append(entry)
        flatten(record,entry)
    df = pd.DataFrame(out)
    return df


dns_cache={}

def fix_ip(ip):
    global dns_cache
    dns = ip
    if ip in dns_cache:
        return dns_cache[ip]
    try:
        addr=reversename.from_address(ip)
        dns = str(resolver.query(addr,"PTR")[0])[:-1]
    except:
        # not an IP
        pass
    dns_cache[ip] = dns
    return dns

def clean_data(data,drop=False):
    to_drop=[]
    for i in range(len(data)):
        # fix dst/src_ip
        src_ip = fix_ip(data.src_ip[i])
        if drop and not 'nersc.gov' in src_ip:
            #data.drop(i)
            to_drop.append(i)
            continue
        dst_ip = fix_ip(data.dst_ip[i])
        src_ip = fix_ip(data.src_ip[i])
        data.loc[i,'src_ip'] = src_ip
        data.loc[i,'dst_ip'] = dst_ip
        data.loc[i,'source'] = src_ip
        data.loc[i,'dest'] = dst_ip
    if drop:
        res = data
        for i in to_drop:
            res = res.drop(i)
        return res
    return data