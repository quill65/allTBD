# python3

# mlabnetdb module

import os, datetime


# put MM database files in module directory
maxMindDbFile = 'GeoIP2-ISP-Blocks-IPv4.%Y%m%d.csv.gz'
maxMindDbFile_latest = 'GeoIP2-ISP-Blocks-IPv4.20180223.csv.gz'


# tree structures for all data; will create multiple versions for date snapshots
__bmaps = [{} for _ in range(33)] # ordered /0 (unsed) to /32

__bmapLoaded = False



def ipv4mask(nbits=32):
    m32 = 0xFFFFFFFF
    if nbits > 32:
        raise Exception("too many bits in mask")
    if nbits==0:
        return 0
    return ~(2**(32-nbits)-1) & m32

def ipv4FromString(ips):
    """convert IPv4 address string to integer"""
    octets = [int(o) for o in ips.split('.')]
    while len(octets) < 4:
        octets.append(0)

    b = 0
    for o in octets:
        b = b << 8
        b |= o

    return b


def __addRecord(db, blockstr, record):

    (ips, nbits) = blockstr.split('/')
    nbits = int(nbits)
    mask = ipv4mask(nbits)
    block = ipv4FromString(ips)
    block &= mask
    #print(str.format('{:08x}', block) + ' / ' + str(nbits))

    db[nbits][block] = record


def __loadMaxMindCsvFile(valid_from=None, valid_to=None):

    global __bmapLoaded, __bmaps

    dbfn = os.path.join(os.path.dirname(__file__), maxMindDbFile_latest)

    # read database file
#    for b in blocks:
#
#        __addRecord(__bmaps, b, 'read from file')

       
    __bmapLoaded = True


##########################################################################################


def lookup(ip, date=None):
    """Given an IP address string (ipv4 or ipv6 format), lookup block entry in GeoIP2
    MaxMind database.  Returns dictionary for fields in this database:
    {network,isp,organization,autonomous_system_number,autonomous_system_organization}
    (string, string, string, int, string).  Or None if n/a.
    """

    if not __bmapLoaded:
        __loadMaxMindCsvFile()

    if not __bmapLoaded:
        raise Exception("failed to load database files")

    for nbits in range(0, 33):
        bmap = __bmaps[nbits]
        pfx = ipv4FromString(ip) & ipv4mask(nbits)
        if pfx in bmap:
            return bmap[pfx]
    return None




# test code
if __name__ == "__main__":

    blocks = ['1.0.216.1/22','192.168.0/24', '88.0.215.0/24','4.20.230.0/23','5.196.64.0/19','12.65.0.0/16','99.0.0.0/8', '192.0.0.0/2']
    for b in blocks:
        __addRecord(__bmaps, b, b)
    __bmapLoaded = True

    for o in __bmaps:
        print(o)


    ips = ['1.0.217.22', '128.99.44.2', '88.0.215.1', '88.0.214.1']

    for ip in ips:
        print(ip , ': ' , lookup(ip))


    b = '0.0.0.0/0'
    __addRecord(__bmaps, b, b)
    for ip in ips:
        print(ip , ': ' , lookup(ip))
