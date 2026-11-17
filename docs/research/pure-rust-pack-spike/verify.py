"""Standalone research check, deliberately outside the ghfs test suite."""
from pathlib import Path
import subprocess
import sys

exe=Path(sys.argv[1]).resolve()
root=Path(sys.argv[2]).resolve()
subprocess.run([sys.executable,str(Path(__file__).with_name("generate.py")),str(root)],check=True)
for mb in (16,128):
 result=subprocess.run([str(exe),str(root/f"{mb}.pack"),str(root/f"out-{mb}")],capture_output=True,text=True,check=True)
 assert result.stdout==(root/f"{mb}.expected").read_text(),result.stdout
 print(f"{mb} MiB: all OIDs and sizes match; {result.stderr.strip()}")
result=subprocess.run([str(exe),str(root/"incompressible.pack"),str(root/"out-incompressible")],capture_output=True,text=True,check=True)
assert result.stdout==(root/"incompressible.expected").read_text(),result.stdout
print(f"16 MiB incompressible: OID and size match; {result.stderr.strip()}")
for name in ("bad-checksum","reserved","copy-outside-base","truncated-insert","short-result"):
 result=subprocess.run([str(exe),str(root/f"{name}.pack"),str(root/name)],capture_output=True,text=True)
 assert result.returncode!=0,f"malformed {name} accepted"
 print(f"{name}: rejected ({result.stderr.strip()})")
