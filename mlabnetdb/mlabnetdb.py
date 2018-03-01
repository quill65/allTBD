
# mlabnetdb module

import os, os.path
import csv, datetime


# put MM database files in module directory
maxMindDbFiles = []
maxMindDbFile = 'GeoIP2-ISP-Blocks-IPv4.20180223.csv.gz'  # %Y%m%d


# tree structures for all data; will create multiple versions for date snapshots
_bmaps = None
_bmapLoaded = None

_m32 = 0xFFFFFFFF

def _init(): # init called once below, called again as needed by unit tests
    global _bmaps, _bmapLoaded
    _bmaps = [{} for _ in range(33)] # ordered /0 (unsed) to /32
    _bmapLoaded = False


def mmDbFile(valid_from=None, valid_to=None):
    # TODO scan directory for date of snapshot
    return maxMindDbFile


def ipv4mask(nbits=32):
    if nbits > 32:
        raise Exception("too many bits in mask")
    if nbits==0:
        return 0
    return ~(2**(32-nbits)-1) & _m32

def ipv4FromString(ips):
    """convert IPv4 address string to integer"""
    octets = [int(o) for o in ips.split('.')]
    while len(octets) < 4:
        octets.append(0)

    b = 0
    for o in octets:
        if o < 0 or o > 255:
            raise ValueError("bad IP address value")
        b = b << 8
        b |= o

    return b & _m32


def _addRecord(db, blockstr, record):

    (ips, nbits) = blockstr.split('/')
    nbits = int(nbits)
    mask = ipv4mask(nbits)
    block = ipv4FromString(ips)
    block &= mask
    #print(str.format('{:08x}', block) + ' / ' + str(nbits))

    db[nbits][block] = record



def xformRecord(rec):
    """do any transformations to input data as needed"""

    if 'autonomous_system_number' in rec:
        asn = rec['autonomous_system_number']
        if asn:
            try:
                rec['autonomous_system_number'] = int(asn)
            except ValueError:
                rec['autonomous_system_number'] = None
        else:
            rec['autonomous_system_number'] = None

        return rec


_records_loaded = None

def _loadMaxMindCsvFile(valid_from=None, valid_to=None):

    global _bmapLoaded, _bmaps
    global _records_loaded

    colnames = None

    dbfn = os.path.join(os.path.dirname(__file__), mmDbFile())

    if not os.path.isfile(dbfn):
        raise Exception("can't find database file "+dbfn+".  MaxMind isp blocks file (e.g. GeoIP2-ISP-Blocks-IPv4.20180223.csv.gz) should be placed in this module's directory")

    openFunc = open
    if dbfn.lower().endswith('.gz'):
        import gzip
        openFunc = gzip.open

    _records_loaded = 0
    with openFunc(dbfn, 'rt', newline='') as f:
        reader = csv.DictReader(f)
        for rec in reader:
            if 'network' in rec:
                rec = xformRecord(rec)
                _addRecord(_bmaps, rec['network'], rec)
                _records_loaded += 1
       
    _bmapLoaded = True


##########################################################################################


def lookup(ip, date=None):
    """Given an IP address string (ipv4 or ipv6 format), lookup block entry in GeoIP2
    MaxMind database.  Returns dictionary for fields in this database:
    {network,isp,organization,autonomous_system_number,autonomous_system_organization}
    where autonomous_system_number is an int, all other fields string.
    """

    if not ip:
        raise ValueError('empty/null ip!')

    if not _bmapLoaded:
        _loadMaxMindCsvFile()

    if not _bmapLoaded:
        raise Exception("failed to load database files")

    for nbits in range(0, 33):
        bmap = _bmaps[nbits]
        pfx = ipv4FromString(ip) & ipv4mask(nbits)
        if pfx in bmap:
            return bmap[pfx]
    return None


_init()
