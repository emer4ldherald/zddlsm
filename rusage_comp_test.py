import subprocess
import sys
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import re
import random
import string


def mkdir(dir):
    try:
        os.mkdir(dir)
    except FileExistsError:
        pass
    except PermissionError:
        print(f"Permission denied: Unable to create '{dir}'.")
    except Exception as e:
        print(f"An error occurred: {e}")


def generate_key(key_len):
    chars = string.ascii_letters + string.digits + string.punctuation
    return "".join(random.SystemRandom().choice(chars) for _ in range(key_len))


def generate_test(key_len, size):
    dir = "rusage_tests/"
    mkdir(dir)

    testname = f"test_{key_len}_{size}"
    test_dir = dir + testname
    f = open(test_dir, "w")

    f.write(size + "\n")
    for i in range(0, int(size)):
        f.write(generate_key(int(key_len)) + "\n")

    f.close()


compression = ["uncompressed", "zstd", "md5", "sha256"]
colors = {"uncompressed": "red", "zstd": "green", "md5": "yellow", "sha256": "orange"}

args = sys.argv

if len(args) != 3:
    print("exprected args: [KEYS_BYTE_LEN] [TEST_SIZE]")
    exit(-1)

key_len = args[1]
test_size = args[2]
test_dir = "rusage_tests/"
test_name = "test_" + str(key_len) + "_" + test_size

generate_test(key_len, test_size)

for compression_type in compression:
    subprocess.run(
        [
            "./build/rusage_test",
            str(key_len),
            str(test_size),
            compression_type,
            test_dir,
            test_name,
        ]
    )


def sci_notation(x, pos):
    return f"{x:.0e}"


graphs_time = {}
graphs_mem = {}
x_axis = []

for compression_type in compression:
    test_filename = test_dir + test_name + "_" + compression_type + ".out"
    f = open(test_filename, "r")
    n = int(f.readline())
    if len(x_axis) == 0:
        x_axis = [float(i) for i in list(filter(None, re.split(" |\n", f.readline())))]
    else:
        f.readline()
    graphs_time[compression_type] = [
        float(i) for i in list(filter(None, re.split(" |\n", f.readline())))
    ]
    graphs_mem[compression_type] = [
        float(i) for i in list(filter(None, re.split(" |\n", f.readline())))
    ]

batch_size = x_axis[1] - x_axis[0]

mkdir(test_dir + "png/")
mkdir(test_dir + "eps/")

# axis formatting
formatter = ticker.ScalarFormatter(useMathText=True)
formatter.set_scientific(True)
formatter.set_powerlimits((0, 0))

# time
fig_time, ax_time = plt.subplots()

ax_time.xaxis.set_major_formatter(formatter)

for compression_type in compression:
    ax_time.plot(
        x_axis,
        graphs_time[compression_type],
        color=colors[compression_type],
        label=compression_type,
    )

ax_time.set_xlabel("keys number")
ax_time.set_ylabel(f"time(s) per batch of size {batch_size}")
ax_time.set_title(f"Time(s) for {key_len}-byte keys")
ax_time.legend()
ax_time.grid(True)

plt.locator_params("both", nbins=20)
plt.tight_layout()

fig_time.savefig(test_dir + f"/png/{test_name}_time.png")
fig_time.savefig(test_dir + f"/eps/{test_name}_time.eps", format="eps")

plt.close(fig_time)

# memory
fig_mem, ax_mem = plt.subplots()

ax_mem.xaxis.set_major_formatter(formatter)

for compression_type in compression:
    ax_mem.plot(
        x_axis,
        graphs_mem[compression_type],
        color=colors[compression_type],
        label=compression_type,
    )

ax_mem.set_xlabel("keys number")
ax_mem.set_ylabel(f"memory(MB) per batch of size {batch_size}")
ax_mem.set_title(f"Memory(MB) for {key_len}-byte keys")
ax_mem.legend()
ax_mem.grid(True)

plt.locator_params("both", nbins=20)
plt.tight_layout()

fig_mem.savefig(test_dir + f"/png/{test_name}_memory.png")
fig_mem.savefig(test_dir + f"/eps/{test_name}_memory.eps", format="eps")

plt.close(fig_mem)
