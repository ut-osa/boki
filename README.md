Boki
==================================

Boki is a research FaaS runtime for stateful serverless computing with shared logs.
Boki exports the shared log API to serverless functions, allowing them to manage states with strong consistency, durability, and fault tolerance.
Boki uses [Nightcore](https://github.com/ut-osa/nightcore) as the runtime for serverless functions.

Boki is the pronunciation of "簿記", meaning bookkeeping in Japanese.

### Building Boki ###

Under Ubuntu 20.04, building Boki needs following dependencies installed:
~~~
sudo apt install g++ make cmake pkg-config autoconf automake libtool curl unzip
~~~

Once installed, build Boki with:

~~~
./build_deps.sh
make -j $(nproc)
~~~

### Kernel requirements ###

Boki uses [io_uring](https://en.wikipedia.org/wiki/Io_uring) for asynchronous I/Os.
io_uring is a very new feature in the Linux kernel (introduced in 5.1),
and evolves rapidly with newer Linux kernel version.

Boki require Linux kernel 5.10 or later to run.

### Boki support libraries ###

As presented in our SOSP paper, we build BokiFlow, BokiStore and BokiQueue for serverless use cases of
transactional workflows, durable object storage, and message queues.

`slib` directory in this repository contains implementations of BokiStore and BokiQueue.
For BokiFlow, check out `workloads/workflow` directory in [ut-osa/boki-benchmarks](https://github.com/ut-osa/boki-benchmarks) repository.

### Running Boki's evaluation workloads ###

A separate repository [ut-osa/boki-benchmarks](https://github.com/ut-osa/boki-benchmarks)
includes scripts and detailed instructions on running evaluation workloads presented in our SOSP paper.

### Limitations of the current prototype ###

The shared log API is only exported to functions written in Go.
