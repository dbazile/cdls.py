# cdls.py

I rewrote the C# version of CDLS in Python for the practice.  I did take some creative liberties with the actual implementation in this one to make it more Pythonic.

Usage:

    Options:
      -h, --help        show this help message and exit
      -l, --list        Gets a list of all registered sources.
      -a, --all         Executes a load operation on every registered source in
                        the CDLS.
      -n, --noisy       Outputs more verbose logging info
      -i, --install-db  Installs the database schema
