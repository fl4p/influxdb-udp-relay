"""Decode the fugu firmware's binary symbol-table wire protocol back to InfluxDB
line protocol. Transparently detected in main.py via looks_binary().

Datagram = <compressor_id:1B> <payload>   (id 0 = raw, 1 = tamp).
payload  = (<varint len> <frame>)*        ; frame[0] = FrameT (1=data, 2=table).
  table : (<SID varint> <name\\0> <DT byte>)* <0-SID>      defines symbols
  data  : <SID(meas)> (<SID tagK><SID tagV>)* 0
                       (<SID fieldK><raw LE value>)* 0  <ts_ms varint>
Field datatype is per-symbol (from the table); SID 0 is reserved as a terminator.
One symbol table is kept per source device. Mirrors sym_line_protocol.h in the
fugu-mppt-firmware repo (and etc/influx_binary_proxy.py there).
"""
import struct

try:
    import tamp
except ImportError:
    tamp = None

# WireDT -> (struct fmt, byte width). Str(0) carries no value (appears only as a SID).
_DT = {1: ('?', 1), 2: ('<b', 1), 3: ('<B', 1), 4: ('<h', 2), 5: ('<H', 2),
       6: ('<i', 4), 7: ('<I', 4), 8: ('<e', 2), 9: ('<f', 4), 10: ('<d', 8)}


def looks_binary(data):
    """A binary datagram starts with the compressor id (0x00/0x01); a text
    line-protocol message starts with a printable measurement char (>= 0x20)."""
    return len(data) >= 1 and data[0] in (0, 1)


class _R:
    __slots__ = ('b', 'i')

    def __init__(self, b):
        self.b, self.i = b, 0

    def varint(self):
        v = s = 0
        while True:
            x = self.b[self.i]; self.i += 1
            v |= (x & 0x7F) << s
            if not (x & 0x80):
                return v
            s += 7

    def cstr(self):
        j = self.b.index(0, self.i); s = self.b[self.i:j].decode(); self.i = j + 1; return s

    def byte(self):
        x = self.b[self.i]; self.i += 1; return x

    def peek(self):
        return self.b[self.i]

    def take(self, n):
        x = self.b[self.i:self.i + n]; self.i += n; return x

    def eof(self):
        return self.i >= len(self.b)


def _field(dt, raw):
    v = struct.unpack(_DT[dt][0], raw)[0]
    if dt == 1:
        return 'true' if v else 'false'
    if dt <= 7:
        return f'{v}i'           # influx integer
    return f'{v:.6g}'            # influx float


def decode(data, tables, src):
    """Return influx line-protocol strings from one datagram. tables[src] holds
    the per-device symbol table across calls. A data frame referencing an unknown
    SID (lost-table hole) is skipped; the firmware periodically resends the table
    to resync. Raises only on a tamp datagram when tamp is unavailable."""
    cid = data[0]
    payload = data[1:]
    if cid == 1:
        if tamp is None:
            raise RuntimeError("tamp-compressed datagram but 'tamp' is not installed")
        payload = tamp.decompress(bytes(payload))
    tab = tables.setdefault(src, {})
    out = []
    r = _R(payload)
    while not r.eof():
        frame = _R(r.take(r.varint()))
        ft = frame.byte()
        if ft == 2:                                     # table: define symbols
            while True:
                sid = frame.varint()
                if sid == 0:
                    break
                tab[sid] = (frame.cstr(), frame.byte())
        elif ft == 1:                                   # data point
            try:
                meas = tab[frame.varint()][0]
                tags = []
                while frame.peek() != 0:
                    k = tab[frame.varint()][0]; v = tab[frame.varint()][0]
                    tags.append(f'{k}={v}')
                frame.byte()                            # tag-list terminator
                fields = []
                while frame.peek() != 0:
                    sid = frame.varint(); name, dt = tab[sid]
                    fields.append(f'{name}={_field(dt, frame.take(_DT[dt][1]))}')
                frame.byte()                            # field-list terminator
                ts = frame.varint()
                out.append(f"{meas}{',' + ','.join(tags) if tags else ''} {','.join(fields)} {ts}")
            except KeyError:
                pass                                    # unknown SID -> lost-table hole, skip frame
        # unknown frame types: length-prefixed, so just advance to the next frame
    return out
