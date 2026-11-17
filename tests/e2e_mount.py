#!/usr/bin/env python3
"""Exercise an actual isolated FUSE/macOS NFS mount against Git smart HTTP.

No installed ghfs service/cache is used. Run from a built source snapshot:
  python3 tests/e2e_mount.py --binary target/release/ghfs --root /tmp/ghfs-e2e-new
Add --github for public GitHub and pinned Linux smoke coverage afterwards.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import errno
import hashlib
import http.server
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import urllib.parse


def probe(spec):
    """A killable child performs potentially blocking kernel filesystem calls."""
    path = Path(spec["path"])
    action = spec["action"]
    if action == "digest":
        h = hashlib.sha256()
        size = 0
        with path.open("rb") as f:
            while chunk := f.read(256 * 1024):
                size += len(chunk)
                h.update(chunk)
        return {"sha256": h.hexdigest(), "size": size}
    if action == "tree":
        h = hashlib.sha256()
        names = sorted(os.listdir(path))
        for name in names:
            h.update(name.encode() + b"\0")
            with (path / name).open("rb") as f:
                while chunk := f.read(256 * 1024):
                    h.update(chunk)
        return {"sha256": h.hexdigest(), "count": len(names)}
    if action == "slice":
        with path.open("rb") as f:
            f.seek(spec["offset"])
            data = f.read(spec["length"])
        return {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}
    if action == "stat":
        s = path.lstat()
        return {"mode": s.st_mode, "size": s.st_size,
                "link": os.readlink(path) if stat.S_ISLNK(s.st_mode) else None}
    if action == "list":
        return sorted(os.listdir(path))
    if action == "write":
        with path.open("wb") as f:
            f.write(b"must not be writable")
        return {"unexpected_write": True}
    raise ValueError(action)


if len(sys.argv) == 3 and sys.argv[1] == "--probe":
    try:
        print(json.dumps(probe(json.loads(sys.argv[2]))))
    except OSError as error:
        print(json.dumps({"errno": error.errno, "error": str(error)}))
        sys.exit(1)
    sys.exit(0)


SCRIPT = Path(__file__).resolve()
PROBE_CHILDREN = set()
PROBE_LOCK = threading.Lock()
GIT_ENV = dict(os.environ, GIT_TERMINAL_PROMPT="0", GCM_INTERACTIVE="never")
GIT_FLAGS = ["-c", "user.name=ghfs-e2e", "-c", "user.email=e2e@example.invalid",
             "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false"]


def git(directory, *args, data=None, timeout=120):
    result = subprocess.run(["git", "-C", str(directory), *GIT_FLAGS, *args],
                            input=data, capture_output=True, env=GIT_ENV,
                            timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f"git {args} failed: {result.stderr.decode(errors='replace')}")
    return result.stdout


def spawn_probe(action, path, **kwargs):
    spec = {"action": action, "path": str(path), **kwargs}
    child = subprocess.Popen([sys.executable, str(SCRIPT), "--probe", json.dumps(spec)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    with PROBE_LOCK:
        PROBE_CHILDREN.add(child)
    return child


def finish_probe(child, timeout=120, expected_errno=None):
    try:
        out, err = child.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        child.kill()
        child.communicate(timeout=15)
        raise RuntimeError(f"filesystem probe timed out after {timeout}s")
    with PROBE_LOCK:
        PROBE_CHILDREN.discard(child)
    if not out:
        raise RuntimeError(f"filesystem probe failed ({child.returncode}): {err.decode(errors='replace')}")
    value = json.loads(out)
    if expected_errno is not None:
        assert value.get("errno") in expected_errno, value
    elif child.returncode:
        raise RuntimeError(f"filesystem probe: {value}")
    return value


def read_probe(action, path, timeout=120, expected_errno=None, **kwargs):
    return finish_probe(spawn_probe(action, path, **kwargs), timeout, expected_errno)


class GitHttpServer:
    """Test-owned CGI bridge; git processes are siblings of the daemon."""
    def __init__(self, remote, log):
        self.remote = remote
        self.log = log
        self.lock = threading.Lock()
        self.requests = []
        self.children = set()
        self.stall = False
        self.stalled = threading.Event()
        self.release = threading.Event()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *_):
                pass

            def do_GET(self):
                self.serve_git()

            def do_POST(self):
                self.serve_git()

            def serve_git(self):
                parsed = urllib.parse.urlsplit(self.path)
                length = int(self.headers.get("Content-Length", "0"))
                if length > 1024 * 1024:
                    self.send_error(413)
                    return
                body = self.rfile.read(length)
                wants = re.findall(rb"want ([0-9a-f]{40})", body)
                entry = {"time": time.time(), "method": self.command,
                         "path": parsed.path, "wants": [s.decode() for s in wants]}
                with owner.lock:
                    owner.requests.append(entry)
                    with owner.log.open("a") as stream:
                        stream.write(json.dumps(entry) + "\n")
                if owner.stall and parsed.path == "/test/stalled.git/git-upload-pack" and b"command=fetch" in body:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/x-git-upload-pack-result")
                    self.end_headers()
                    try:
                        self.wfile.write(b"000dpackfile\n")
                        self.wfile.flush()
                        owner.stalled.set()
                        owner.release.wait(120)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return
                env = dict(GIT_ENV, GIT_PROJECT_ROOT=str(owner.remote),
                           GIT_HTTP_EXPORT_ALL="1", PATH_INFO=parsed.path,
                           QUERY_STRING=parsed.query, REQUEST_METHOD=self.command,
                           CONTENT_TYPE=self.headers.get("Content-Type", ""),
                           CONTENT_LENGTH=str(length),
                           HTTP_GIT_PROTOCOL=self.headers.get("Git-Protocol", ""),
                           SERVER_PROTOCOL="HTTP/1.0", REMOTE_ADDR="127.0.0.1")
                process = subprocess.Popen(["git", "http-backend"], env=env,
                                           stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, start_new_session=True)
                with owner.lock:
                    owner.children.add(process)
                # stderr must not fill while stdout is being streamed.
                stderr = bytearray()
                def drain():
                    while chunk := process.stderr.read(4096):
                        if len(stderr) < 65536:
                            stderr.extend(chunk[:65536-len(stderr)])
                drainer = threading.Thread(target=drain, daemon=True)
                drainer.start()
                try:
                    process.stdin.write(body)
                    process.stdin.close()
                    headers = []
                    code = 200
                    total = 0
                    while True:
                        line = process.stdout.readline(65537)
                        total += len(line)
                        if total > 65536 or not line:
                            raise RuntimeError("invalid CGI response headers")
                        if line in (b"\r\n", b"\n"):
                            break
                        name, value = line.decode("latin1").rstrip("\r\n").split(":", 1)
                        if name.lower() == "status":
                            code = int(value.strip().split()[0])
                        else:
                            headers.append((name, value.strip()))
                    self.send_response(code)
                    for name, value in headers:
                        self.send_header(name, value)
                    self.end_headers()
                    while chunk := process.stdout.read(65536):
                        self.wfile.write(chunk)
                    process.wait(timeout=30)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    if process.poll() is None:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        process.wait(timeout=15)
                    process.stdout.close()
                    drainer.join(timeout=2)
                    process.stderr.close()
                    with owner.lock:
                        owner.children.discard(process)

        self.http = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.http.daemon_threads = True
        self.thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.http.server_port}"

    def snapshot(self):
        with self.lock:
            return list(self.requests)

    def fetch_count(self):
        return sum(bool(r["wants"]) for r in self.snapshot())

    def quiet(self, timeout=30):
        previous = None
        steady = time.monotonic()
        deadline = steady + timeout
        while time.monotonic() < deadline:
            count = len(self.snapshot())
            if count != previous:
                previous, steady = count, time.monotonic()
            elif time.monotonic() - steady > 0.5:
                return
            time.sleep(0.1)
        raise RuntimeError("HTTP fixture did not become quiet")

    def close(self):
        self.release.set()
        self.http.shutdown()
        self.http.server_close()
        with self.lock:
            children = list(self.children)
        for process in children:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.thread.join(timeout=5)


def processes():
    out = subprocess.check_output(["ps", "-axo", "pid=,ppid=,rss=,comm="], text=True)
    result = {}
    for line in out.splitlines():
        fields = line.split(None, 3)
        if len(fields) == 4:
            result[int(fields[0])] = (int(fields[1]), int(fields[2]), fields[3])
    return result


def descendants(pid, table):
    found, parents = [], {pid}
    while parents:
        next_parents = set()
        for child, (parent, rss, command) in table.items():
            if parent in parents:
                found.append({"pid": child, "command": command, "rss_kib": rss})
                next_parents.add(child)
        parents = next_parents
    return found


class Daemon:
    def __init__(self, binary, root, results):
        self.binary, self.root, self.results = binary, root, results
        self.mount = root / "mnt"
        self.cache = root / "cache"
        self.process = None
        self.monitor = None
        self.stop_monitor = threading.Event()
        self.git_children = []
        self.peak_rss = 0
        self.runs = 0

    def start(self, base_url):
        self.runs += 1
        env = dict(os.environ, GHFS_MOUNT_POINT=str(self.mount),
                   GHFS_CACHE_DIR=str(self.cache), GHFS_SOCKET=str(self.root / "ghfs.sock"),
                   GHFS_GIT_BASE_URL=base_url, RUST_LOG="ghfs=debug")
        if sys.platform.startswith("linux") and Path("/run/wrappers/bin").is_dir():
            env["PATH"] = "/run/wrappers/bin:" + env.get("PATH", "")
        self.output = (self.root / f"daemon-{self.runs}.log").open("wb")
        self.process = subprocess.Popen([str(self.binary), "daemon"], env=env,
                                        stdout=self.output, stderr=subprocess.STDOUT,
                                        start_new_session=True)
        (self.root / "daemon.pid").write_text(str(self.process.pid))
        self.results.setdefault("daemon_pids", []).append(self.process.pid)
        self.stop_monitor.clear()
        def monitor():
            while not self.stop_monitor.is_set():
                try:
                    table = processes()
                    if self.process.pid in table:
                        self.peak_rss = max(self.peak_rss, table[self.process.pid][1])
                    self.git_children.extend(p for p in descendants(self.process.pid, table)
                                             if Path(p["command"]).name.startswith("git"))
                except (OSError, subprocess.SubprocessError):
                    pass
                self.stop_monitor.wait(0.1)
        self.monitor = threading.Thread(target=monitor, daemon=True)
        self.monitor.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"daemon exited {self.process.returncode}; see {self.output.name}")
            if (self.root / "ghfs.sock").exists():
                try:
                    if self.is_mounted():
                        return
                except subprocess.SubprocessError:
                    pass
            time.sleep(0.1)
        raise RuntimeError(f"scratch mount did not become ready; see {self.output.name}")

    def is_mounted(self):
        # Read the OS mount table, avoiding a potentially hung stat on stale NFS.
        if sys.platform.startswith("linux"):
            target = str(self.mount).replace(" ", "\\040")
            return any(line.split()[1] == target for line in Path("/proc/mounts").read_text().splitlines())
        out = subprocess.check_output(["/sbin/mount"], text=True, timeout=10)
        return f" on {self.mount} (" in out

    def stop(self, kill=False):
        process = self.process
        if process is None:
            return
        if process.poll() is None:
            process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        self.stop_monitor.set()
        if self.monitor:
            self.monitor.join(timeout=5)
        self.output.close()
        if self.is_mounted():
            candidates = (["/sbin/umount", "-f", str(self.mount)],) if sys.platform == "darwin" else (
                ["/run/wrappers/bin/fusermount3", "-uz", str(self.mount)],
                ["fusermount3", "-uz", str(self.mount)], ["fusermount", "-uz", str(self.mount)])
            for command in candidates:
                try:
                    subprocess.run(command, timeout=20, capture_output=True)
                    if not self.is_mounted():
                        break
                except (FileNotFoundError, subprocess.TimeoutExpired):
                    continue
            if self.is_mounted():
                raise RuntimeError(f"could not clean owned scratch mount {self.mount}")
        self.process = None

    def check_storage(self):
        repos = self.cache / "v3" / "repos"
        assert repos.is_dir(), f"new v3 repository store absent: {repos}"
        packs = [str(p.relative_to(self.cache)) for p in repos.rglob("*")
                 if p.is_file() and p.suffix in (".pack", ".idx") and "objects" in p.parts]
        assert not packs, packs
        assert not self.git_children, self.git_children
        self.results["peak_daemon_rss_kib"] = self.peak_rss
        self.results["git_descendants_observed"] = self.git_children
        self.results["retained_packs_and_indexes"] = packs
        self.results["loose_object_count"] = sum(1 for p in repos.rglob("objects/[0-9a-f][0-9a-f]/*") if p.is_file())


def make_fixture(root):
    work, remote = root / "work", root / "remote"
    work.mkdir()
    (remote / "test").mkdir(parents=True)
    git(work, "init", "-q", "-b", "main")
    files = {"README.md": b"ghfs native mount e2e\n",
             "nested/deep/message.txt": b"nested immutable content\n",
             "bin/tool": b"#!/bin/sh\nprintf ghfs-e2e\\n\n"}
    for name, contents in files.items():
        path = work / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
    (work / "bin/tool").chmod(0o755)
    (work / "nested/link").symlink_to("deep/message.txt")
    (work / "huge").mkdir()
    for i in range(1500):
        (work / "huge" / f"f{i:04}.txt").write_text(f"file {i}: hermetic mount contents\n")
    (work / "large").mkdir()
    for i in range(4):
        generator = random.Random(1000 + i)
        with (work / "large" / f"blob{i}.bin").open("wb") as output:
            for _ in range(12 * 1024 * 1024 // 65536):
                output.write(generator.randbytes(65536))
    git(work, "add", ".")
    git(work, "update-index", "--add", "--cacheinfo", "160000", "1" * 40, "missing-submodule")
    git(work, "commit", "-qm", "mount fixture")
    head = git(work, "rev-parse", "HEAD").decode().strip()
    git(work, "tag", "-a", "v1", "-m", "annotated mount tag")
    git(work, "checkout", "-qb", "feature/one")
    (work / "branch.txt").write_bytes(b"feature branch only\n")
    git(work, "add", "branch.txt")
    git(work, "commit", "-qm", "feature fixture")
    feature = git(work, "rev-parse", "HEAD").decode().strip()
    git(work, "checkout", "-q", "main")
    for name in ("project", "stalled"):
        bare = remote / "test" / f"{name}.git"
        git(remote, "clone", "--bare", "-q", str(work), str(bare))
        git(bare, "config", "uploadpack.allowFilter", "true")
        git(bare, "config", "uploadpack.allowAnySHA1InWant", "true")
    expected = {}
    for name in [*files, *(f"large/blob{i}.bin" for i in range(4))]:
        oid = git(work, "rev-parse", f"{head}:{name}").decode().strip()
        data = git(work, "cat-file", "blob", oid)
        expected[name] = {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data), "oid": oid}
    tree_lines = git(work, "ls-tree", "-rz", head, "huge").split(b"\0")
    tree_entries = []
    for line in tree_lines:
        if line:
            header, name = line.split(b"\t", 1)
            tree_entries.append((name.decode().removeprefix("huge/"), header.split()[2].decode()))
    batch = git(work, "cat-file", "--batch", data="".join(oid + "\n" for _, oid in tree_entries).encode())
    position, aggregate = 0, hashlib.sha256()
    for name, oid in tree_entries:
        end = batch.index(b"\n", position)
        found, kind, size = batch[position:end].split()
        assert found.decode() == oid and kind == b"blob"
        size = int(size)
        contents = batch[end+1:end+1+size]
        aggregate.update(name.encode() + b"\0" + contents)
        position = end + 1 + size + 1
    assert position == len(batch)
    return {"work": work, "remote": remote, "head": head, "feature": feature,
            "expected": expected, "huge_sha256": aggregate.hexdigest(),
            "huge_oids": {oid for _, oid in tree_entries},
            "nested_tree": git(work, "rev-parse", f"{head}:nested/deep").decode().strip()}


def expect_digest(path, expected):
    got = read_probe("digest", path)
    assert got == {k: expected[k] for k in ("sha256", "size")}, (str(path), got, expected)
    return got


def hermetic(daemon, fixture, server, results):
    mount = daemon.mount
    project = mount / "test/project"
    expected = fixture["expected"]
    daemon.start(server.url)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        reads = [pool.submit(expect_digest, project / name, expected[name]) for name in
                 ["README.md", "nested/deep/message.txt", *(f"large/blob{i}.bin" for i in range(4)), "README.md", "nested/deep/message.txt"]]
        for future in reads:
            future.result()
    for selector in (fixture["head"], "main", "v1"):
        expect_digest(mount / "by-ref/test/project" / selector / "README.md", expected["README.md"])
    branch_data = git(fixture["work"], "cat-file", "blob", f"{fixture['feature']}:branch.txt")
    expect_digest(mount / "by-ref/test/project/feature%2Fone/branch.txt",
                  {"sha256": hashlib.sha256(branch_data).hexdigest(), "size": len(branch_data)})
    executable = read_probe("stat", project / "bin/tool")
    assert executable["mode"] & stat.S_IXUSR, executable
    link = read_probe("stat", project / "nested/link")
    assert stat.S_ISLNK(link["mode"]) and link["link"] == "deep/message.txt", link
    expect_digest(project / "nested/link", expected["nested/deep/message.txt"])
    submodule = read_probe("stat", project / "missing-submodule")
    assert stat.S_ISREG(submodule["mode"]) and submodule["size"] == 0, submodule
    read_probe("digest", project / "missing.txt", expected_errno={errno.ENOENT})
    read_probe("digest", mount / "absent/missing/README", expected_errno={errno.ENOENT})
    read_probe("write", project / "README.md", expected_errno={errno.EROFS, errno.EACCES})
    names = read_probe("list", project / "huge")
    assert len(names) == 1500 and names[0] == "f0000.txt" and names[-1] == "f1499.txt"
    huge = read_probe("tree", project / "huge", timeout=180)
    assert huge == {"sha256": fixture["huge_sha256"], "count": 1500}, huge
    oid = expected["large/blob0.bin"]["oid"]
    blob = git(fixture["work"], "cat-file", "blob", oid)
    tail = blob[-4097:]
    got = read_probe("slice", project / "large/blob0.bin", offset=len(blob)-4097, length=8192)
    assert got == {"sha256": hashlib.sha256(tail).hexdigest(), "size": len(tail)}, got
    server.quiet()
    requests = server.snapshot()
    want_sets = [r["wants"] for r in requests if r["wants"]]
    assert want_sets and max(map(len, want_sets)) <= 1000
    assert max(map(len, want_sets)) > 1, "no batched fetch observed"
    assert fixture["huge_oids"] <= {oid for wants in want_sets for oid in wants}, "huge directory wants incomplete"
    before = server.fetch_count()
    expect_digest(project / "README.md", expected["README.md"])
    expect_digest(project / "large/blob0.bin", expected["large/blob0.bin"])
    assert server.fetch_count() == before, "warm reads caused an object fetch"
    results["hermetic"] = {"parallel_first_reads": 8, "huge_directory_files": 1500,
                           "large_blob_bytes": len(blob), "initial_fetches": before,
                           "maximum_wants": max(map(len, want_sets)), "warm_read_fetches": 0}
    daemon.check_storage()
    daemon.stop()

    repo = daemon.cache / "v3/repos/test/project.git"
    marker = repo / "ghfs-complete" / fixture["head"]
    assert marker.is_file(), f"completion marker absent: {marker}"
    tree = fixture["nested_tree"]
    tree_path = repo / "objects" / tree[:2] / tree[2:]
    assert tree_path.is_file(), tree_path
    # Keep the marker while corrupting a descendant, precisely the fast-path risk.
    tree_path.write_bytes(b"corrupt nested tree after completed fetch")
    abandoned = repo / "ghfs-staging/old-transfer"
    abandoned.mkdir(parents=True)
    (abandoned / "partial.pack").write_bytes(b"interrupted")
    daemon.start(server.url)
    expect_digest(project / "nested/deep/message.txt", expected["nested/deep/message.txt"])
    assert not abandoned.exists(), "startup left abandoned staging"
    assert tree_path.read_bytes() != b"corrupt nested tree after completed fetch"
    assert marker.is_file()
    daemon.stop()
    # Also cover an absent descendant with the completed marker still present.
    tree_path.unlink()
    daemon.start(server.url)
    expect_digest(project / "nested/deep/message.txt", expected["nested/deep/message.txt"])
    assert tree_path.is_file() and marker.is_file()
    results["restart_recovery"] = {"corrupt_nested_tree": True, "missing_nested_tree": True,
                                   "abandoned_staging_cleaned": True}

    server.stall = True
    blocked = spawn_probe("digest", mount / "test/stalled/README.md")
    try:
        assert server.stalled.wait(30), "controlled fetch did not reach stall"
        expect_digest(project / "README.md", expected["README.md"])
        daemon.stop(kill=True)
    finally:
        server.stall = False
        server.release.set()
        if blocked.poll() is None:
            blocked.kill()
        blocked.communicate(timeout=20)
    daemon.start(server.url)
    expect_digest(mount / "test/stalled/README.md", expected["README.md"])
    results["crash_during_http_fetch"] = {"cached_read_while_stalled": True, "recovered": True}
    daemon.check_storage()
    daemon.stop()


def github(daemon, results):
    daemon.start("https://github.com")
    hello = b"Hello World!\n"
    want = {"sha256": hashlib.sha256(hello).hexdigest(), "size": len(hello)}
    expect_digest(daemon.mount / "octocat/Hello-World/README", want)
    expect_digest(daemon.mount / "by-ref/octocat/Hello-World/7fd1a60b01f91b314f59955a4e4d4e80d8edf11d/README", want)
    refs = subprocess.run(["git", "ls-remote", "https://github.com/torvalds/linux.git", "HEAD"],
                          capture_output=True, timeout=60, env=GIT_ENV, check=True).stdout
    commit = refs.split()[0].decode()
    assert re.fullmatch("[0-9a-f]{40}", commit)
    base = daemon.mount / "by-ref/torvalds/linux" / commit
    samples = ["README", "Makefile", "drivers/Makefile", "net/ipv4/tcp.c"]
    # Independent reference bytes from Git itself (raw.githubusercontent.com
    # rate-limits cloud IPs). These git processes belong to the test, not ghfs.
    reference = daemon.root / "linux-reference"
    git(daemon.root, "clone", "-q", "--depth=1", "--filter=blob:none", "--no-checkout",
        "https://github.com/torvalds/linux.git", str(reference), timeout=600)
    git(reference, "fetch", "-q", "--depth=1", "origin", commit, timeout=600)
    for name in samples:
        data = git(reference, "cat-file", "blob", f"{commit}:{name}", timeout=300)
        expect_digest(base / name, {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data)})
    listing = read_probe("list", base / "include/linux", timeout=180)
    assert len(listing) > 1000, len(listing)
    results["github"] = {"linux_commit": commit, "verified_files": samples,
                         "linux_include_entries": len(listing), "octocat_pinned_and_head": True}
    daemon.check_storage()
    daemon.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--github", action="store_true")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        parser.error(f"--root must be empty so all data is task-owned: {root}")
    if len(os.fsencode(root / "ghfs.sock")) >= 100:
        parser.error("--root makes the Unix socket path too long")
    for name in ("mnt", "cache"):
        (root / name).mkdir()
    sentinel = root / "cache/v2/e2e-preserve-sentinel"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text("existing v2 cache must remain untouched\n")
    (root / "e2e-owner.json").write_text(json.dumps({"pid": os.getpid(), "binary": str(binary)}))
    results = {"platform": sys.platform, "binary": str(binary), "root": str(root),
               "started_at": time.time(), "status": "running"}
    daemon = Daemon(binary, root, results)
    server = None
    started = time.monotonic()
    try:
        fixture = make_fixture(root)
        results["fixture_commit"] = fixture["head"]
        server = GitHttpServer(fixture["remote"], root / "http.jsonl")
        hermetic(daemon, fixture, server, results)
        if args.github:
            github(daemon, results)
        assert sentinel.read_text() == "existing v2 cache must remain untouched\n"
        results["v2_sentinel_preserved"] = True
        results["status"] = "passed"
    except BaseException as error:
        results["status"] = "failed"
        results["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        try:
            daemon.stop()
        finally:
            if server:
                server.close()
            with PROBE_LOCK:
                children = list(PROBE_CHILDREN)
            for child in children:
                if child.poll() is None:
                    child.kill()
                try:
                    child.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    pass
            results["duration_seconds"] = round(time.monotonic() - started, 3)
            (root / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(json.dumps(results, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
