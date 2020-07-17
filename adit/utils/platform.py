import os
import platform
import psutil

__all__ = ['is_windows', 'is_arm', 'is_linux', 'is_x64', 'is_x86', 'is_macos', 'get_platform', 'get_ncores', 'get_nthreads']

OS_NAME = os.name  # nt
P_SYSTEM = platform.system()  # Windows
P_ARCH = platform.architecture()  # ('64bit', 'WindowsPE')
P_RELEASE = platform.release()  # 10
P_PYVERSION = platform.python_version()  # 3.7.7
P_MACHINE = platform.machine()  # AMD64
P_NODE = platform.node()  # BEARCHEN
P_PLATFORM = platform.platform()  # Windows-10-10.0.19577-SP0


def is_windows() -> bool:
    return 'windows' in P_SYSTEM.lower()


# TODO: support macos
def is_macos() -> bool:
    return ""


def is_linux() -> bool:
    return 'linux' in P_SYSTEM.lower()


def is_x64() -> bool:  # 64 bit platform
    return '64' in P_ARCH[0] or '64' in P_MACHINE


# TODO: support x86
def is_x86() -> bool:  # 32 bit platform
    return ""


# TODO: support arm
def is_arm() -> bool:
    return False


def get_platform() -> str:
    os = "windows" if is_windows() else "linux" if is_linux() else "macos" if is_macos() else "unknown"
    arch = "x64" if is_x64() else "x86" if is_x86() else "arm" if is_arm() else "00"
    return os + "_" + arch


def get_ncores() -> int:
    return psutil.cpu_count()


# TODO: if cpu supprot hyper threading, the number of threads = N*ncores
def get_nthreads() -> int:
    return get_ncores()
