# pendolino

Bidirectional named pipe to tcp bridge.

This program acts as a bridge between a [Windows named pipe](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipes) and a TCP socket. It is used for connecting a serial port on a Hyper-V hypervisor to an [openQA](https://open.qa/) instance.

## Usage

```
pendolino PIPE [BINDADDRESS]
  PIPE                    Path to the named pipe
  BINDADDRESS             Local address to bind listening tcp socket to
```

To connect to a Hyper-V instance, one needs to first add a COM to named pipe option to the virtual machine, e.g. for a VM named `jellyfish` one could run:

```
Set-VMComPort -VMName jellyfish -Number 1 -Path \\.\pipe\jellyfish
```

Afterwards `pendolino` can bridge the serial port to a tcp socket on port 2001 via

```
pendolino -v \\.\pipe\jellyfish 127.0.0.1:2001
```

## Credits

* Inspired by https://github.com/pratikpc/named-pipe-to-tcp-proxy

