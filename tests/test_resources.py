"""CPUs and memory this process may use: CPU affinity and container (cgroup) limits."""

import os

import pytest

from pqlint import compact as C
from pqlint import suggest as S

GiB = 1024**3


@pytest.fixture
def cgroup(tmp_path, monkeypatch):
    """An empty, fake /sys/fs/cgroup; tests drop the files they need into it."""
    monkeypatch.setattr(C, "CGROUP_ROOT", tmp_path)
    monkeypatch.setattr(C, "_meminfo_available", lambda: 8 * GiB)

    def put(name, text):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    return put


# --- memory ---------------------------------------------------------------------------


def test_no_cgroup_files_means_meminfo_only(cgroup):
    assert C.available_memory() == 8 * GiB


def test_cgroup_v2_limit_minus_usage(cgroup):
    cgroup("memory.max", f"{2 * GiB}\n")
    cgroup("memory.current", f"{GiB // 2}\n")
    assert C.available_memory() == int(1.5 * GiB)


def test_the_smaller_of_meminfo_and_cgroup_wins(cgroup):
    cgroup("memory.max", f"{64 * GiB}\n")
    cgroup("memory.current", "0\n")
    assert C.available_memory() == 8 * GiB  # the host has less than the limit allows


def test_cgroup_v2_unlimited_is_ignored(cgroup):
    cgroup("memory.max", "max\n")
    cgroup("memory.current", "123\n")
    assert C.available_memory() == 8 * GiB


def test_cgroup_v1_limit_minus_usage(cgroup):
    cgroup("memory/memory.limit_in_bytes", f"{GiB}\n")
    cgroup("memory/memory.usage_in_bytes", f"{GiB // 4}\n")
    assert C.available_memory() == int(0.75 * GiB)


def test_cgroup_v1_unlimited_sentinel_is_ignored(cgroup):
    cgroup("memory/memory.limit_in_bytes", "9223372036854771712\n")
    cgroup("memory/memory.usage_in_bytes", "5\n")
    assert C.available_memory() == 8 * GiB


def test_usage_above_the_limit_leaves_nothing(cgroup):
    cgroup("memory.max", f"{GiB}\n")
    cgroup("memory.current", f"{2 * GiB}\n")
    assert C.available_memory() == 0


@pytest.mark.parametrize("junk", ["", "garbage", "-1", "1.5e9"])
def test_unreadable_cgroup_values_are_ignored(cgroup, junk):
    cgroup("memory.max", junk)
    cgroup("memory.current", junk)
    assert C.available_memory() == 8 * GiB


def test_cgroup_alone_when_meminfo_is_unknown(cgroup, monkeypatch):
    monkeypatch.setattr(C, "_meminfo_available", lambda: None)
    cgroup("memory.max", f"{GiB}\n")
    cgroup("memory.current", "0\n")
    assert C.available_memory() == GiB


def test_nothing_known_is_none(cgroup, monkeypatch):
    monkeypatch.setattr(C, "_meminfo_available", lambda: None)
    assert C.available_memory() is None


# --- CPUs -----------------------------------------------------------------------------


@pytest.fixture
def cpus(cgroup, monkeypatch):
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: set(range(16)), raising=False)
    return cgroup


def test_affinity_limits_the_cpus(cpus, monkeypatch):
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: {0, 1, 5})
    assert C.available_cpus() == 3


def test_cpu_quota_v2_rounds_up(cpus):
    cpus("cpu.max", "150000 100000\n")
    assert C.available_cpus() == 2


def test_cpu_quota_v2_unlimited_falls_back_to_affinity(cpus):
    cpus("cpu.max", "max 100000\n")
    assert C.available_cpus() == 16


def test_cpu_quota_v1(cpus):
    cpus("cpu/cpu.cfs_quota_us", "200000\n")
    cpus("cpu/cpu.cfs_period_us", "100000\n")
    assert C.available_cpus() == 2


def test_cpu_quota_v1_unlimited(cpus):
    cpus("cpu/cpu.cfs_quota_us", "-1\n")
    cpus("cpu/cpu.cfs_period_us", "100000\n")
    assert C.available_cpus() == 16


def test_quota_larger_than_affinity_does_not_add_cpus(cpus, monkeypatch):
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: {0, 1})
    cpus("cpu.max", "800000 100000\n")
    assert C.available_cpus() == 2


@pytest.mark.parametrize("junk", ["", "garbage", "0 100000", "100000 0", "-5 100000"])
def test_bad_cpu_quota_is_ignored(cpus, junk):
    cpus("cpu.max", junk)
    assert C.available_cpus() == 16


def test_at_least_one_cpu(cpus):
    cpus("cpu.max", "1000 100000\n")  # a tenth of a CPU
    assert C.available_cpus() == 1


def test_without_affinity_support_uses_cpu_count(cgroup, monkeypatch):
    monkeypatch.delattr(os, "sched_getaffinity", raising=False)
    monkeypatch.setattr(os, "cpu_count", lambda: 6)
    assert C.available_cpus() == 6


def test_suggest_threads_follow_the_available_cpus(monkeypatch):
    monkeypatch.setattr(S, "available_cpus", lambda: 3)
    assert S.resolve_threads(None, 1, 10 * GiB) == 3
