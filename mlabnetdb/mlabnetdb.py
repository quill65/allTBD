#!/usr/bin/python3

import datetime



bmaps = [{}] * 33 # ordered /0 (unsed) to /32


def ipv4mask(nbits=32):
    m32 = 0xFFFFFFFF
    if nbits > 32:
        raise Exception("too many bits in mask")
    if nbits==32:
        return m32
    return ~(2**(32-nbits)-1) & m32

def ipv4FromString(ips):

    octets = [int(o) for o in ips.split('.')]
    while len(octets) < 4:
        octets.append(0)

    b = 0
    for o in octets:
        b = b << 8
        b |= o

    return b


def lookup(ip, date=None):
    for nbits in range(1, 33):
        bmap = bmaps[nbits]
        pfx = ipv4FromString(ip) & ipv4mask(nbits)
        if pfx in bmap:
            return bmap[pfx]
    return None



def loadMaxMindCsvFile(fn, valid_from=None, valid_to=None):

    blocks = ['1.0.216.1/22','88.0.215.0/24','4.20.230.0/23','5.196.64.0/19','12.65.0.0/16','99.0.0.0/8', '192.0.0.0/2']

    # read database file
    for b in blocks:

        (ips, nbits) = b.split('/')
        nbits = int(nbits)
        mask = ipv4mask(nbits)
        block = ipv4FromString(ips)
        block &= mask

        #print(str.format('{:08x}', block) + ' / ' + str(nbits))
       
        bmaps[nbits][block] = 'block ' + str.format('{:08x}', block) + '/' + str(nbits)


ips = ['1.0.217.22', '128.99.44.2', '88.0.215.1', '88.0.214.1']

for ip in ips:
    print(ip , ': ' , lookup(ip))





def asnInfoFromIp(ip):
    """Given an IP address string (ipv4 or ipv6 format), lookup block entry in GeoIP2
    MaxMind database.  Returns dictionary for fields in this database:
    {network,isp,organization,autonomous_system_number,autonomous_system_organization}
    (string, string, string, int, string).  Or None if n/a.
    """

    if not ip:
        return None

#    if '.' in ip:
#        return _ipv4AsnLookup(
