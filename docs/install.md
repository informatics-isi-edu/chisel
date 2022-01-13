# Installation

You can either install quickly from the source repository using Python's `pip`
package manager, or you can clone the source first which has the advantage of 
getting the examples and tests.

### System Requirements

- Python 3.7+
- [setuptools](https://pypi.org/project/setuptools/)
- `pip` and `git` for installation

### Quick install

Install the library from the [PyPI](https://pypi.org/) software repository.

```sh
$ pip install deriva-chisel
```

Install the library directly from its source code repository.

```sh
$ pip install https://github.com/informatics-isi-edu/chisel.git
```

For system-wide installations, use `sudo` and execute the command without the 
`--user` option. You will _not_ get the `examples` and `tests` with the quick 
install instructions.

### Clone and install

This installation method gets a copy of the source and then installs it.

1. Clone the source repository
    ```sh
    $ git clone https://github.com/informatics-isi-edu/chisel.git
    ```
2. Install
    ```sh
    $ cd chisel
    $ pip install -e .
    ```
    You may need to use `sudo` for system-wide install or add the `--user` option 
    for current user only install. This examples uses the `-e` option which means
    that the installation simply references your `chisel` local repository. That 
    way you can quickly update to the latest code just by performing a `git pull`
    on your local `chisel` repo.
3. Run the tests
    ```sh
    $ export DERIVA_PY_TEST_HOSTNAME=www.example.org
    $ python -m unittest discover
    ```
    See [the notes below on setting environment variables for testing](#testing). 
    Note that there may be transient network errors during the running of the tests 
    but if the final status of the tests reads `OK` then the CHiSEL tests have run 
    successfully. The final lines of the output should look something like this, though 
    the total number of tests may change as we add new tests.
    ```sh
    ....................s....s.......................
    ----------------------------------------------------------------------
    Ran 102 tests in 36.071s
    
    OK (skipped=2)
    ```
    Some expensive tests are skipped by default but can be enabled by setting 
    additional environment variables.
4. See examples in the [`./examples` directory](./examples) of this repository.

### Testing

The package includes unit tests. They may be run without any configuration, 
however, certain test cases and suites will be skipped without the following
environment variables defined.

* `DERIVA_PY_TEST_HOSTNAME`:
  To run the ERMrest catalog test suite, set this variable to the hostname of
  a server running an ERMrest service. You will also need to establish valid
  user credentials (e.g., by using the Deriva-Auth client).
* `DERIVA_PY_TEST_CATALOG`:
  In addition, set this variable to reuse a catalog. This variable is typically
  only used during development activities that would motivate frequently
  repeated test runs.
* `CHISEL_TEST_ALL`:
  Set this variable (any value will do) to run all tests rather than skipping the
  most expensive tests.
