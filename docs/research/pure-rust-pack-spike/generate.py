
import zlib,hashlib,struct,sys
from pathlib import Path
root=Path(sys.argv[1]); root.mkdir(parents=True,exist_ok=True)
def var(n):
 b=bytearray()
 while n>=128:b.append((n&127)|128);n>>=7
 b.append(n);return b
def hdr(typ,n):
 b=bytearray([(typ<<4)|(n&15)]);n>>=4
 if n:b[-1]|=128
 while n:
  b.append(n&127);n>>=7
  if n:b[-1]|=128
 return b
def ofs(n):
 b=bytearray([n&127])
 while n>>7:
  n=(n>>7)-1;b.insert(0,128|(n&127))
 return b
for mb in (16,128):
 n=mb*1024*1024
 h=hashlib.sha1(f'blob {n}\0'.encode());chunk=b'a'*65536
 for _ in range(n//len(chunk)):h.update(chunk)
 oid=h.digest()
 # A forward ref-delta copies first 64KiB; base blob follows it.
 delta=bytes(var(n)+var(65537)+b'\x80\x01b')
 data=bytearray(b'PACK'+struct.pack('>II',2,3));data+=hdr(7,len(delta))+oid+zlib.compress(delta)
 base_offset=len(data);data+=hdr(3,n);z=zlib.compressobj()
 for _ in range(n//len(chunk)):data+=z.compress(chunk)
 data+=z.flush()
 # A backward ofs-delta copies first 64KiB and appends c.
 offset=len(data);delta=bytes(var(n)+var(65537)+b'\x80\x01c');data+=hdr(6,len(delta))+ofs(offset-base_offset)+zlib.compress(delta)
 data+=hashlib.sha1(data).digest();(root/f'{mb}.pack').write_bytes(data)
 expected=[hashlib.sha1(b'blob 65537\0'+chunk+b'b').hexdigest()+' 65537',oid.hex()+f' {n}',hashlib.sha1(b'blob 65537\0'+chunk+b'c').hexdigest()+' 65537'];(root/f'{mb}.expected').write_text('\n'.join(expected)+'\n')

# Malformed inputs keep valid outer pack checksums so the interpreter is tested.
base=b'ab'; oid=hashlib.sha1(b'blob 2\0ab').digest()
for name,instr in {'reserved':b'\x02\x01\x00','copy-outside-base':b'\x02\x01\x91\x03\x01','truncated-insert':b'\x02\x01\x01','short-result':b'\x02\x02\x01x'}.items():
 data=bytearray(b'PACK'+struct.pack('>II',2,2));data+=hdr(7,len(instr))+oid+zlib.compress(instr);data+=hdr(3,2)+zlib.compress(base);data+=hashlib.sha1(data).digest();(root/f'{name}.pack').write_bytes(data)
data=bytearray((root/'16.pack').read_bytes());data[20]^=1;(root/'bad-checksum.pack').write_bytes(data)

# A large compressed entry must not turn into a whole-entry allocation either.
import os
n=16*1024*1024
pack_hash=hashlib.sha1(); object_hash=hashlib.sha1(f'blob {n}\0'.encode())
with (root/'incompressible.pack').open('wb') as f:
 def emit(data):f.write(data);pack_hash.update(data)
 emit(b'PACK'+struct.pack('>II',2,1));emit(hdr(3,n));z=zlib.compressobj()
 for _ in range(n//65536):
  chunk=os.urandom(65536);object_hash.update(chunk);emit(z.compress(chunk))
 emit(z.flush());f.write(pack_hash.digest())
(root/'incompressible.expected').write_text(object_hash.hexdigest()+f' {n}\n')
