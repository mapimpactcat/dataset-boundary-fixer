# (OUT OF DATE) install instructions
[These instructions should no longer be necessary, just do `poetry install`, but I leave it here in case it's still useful]
This project uses the `psycopg2` postgres bindings, which require libssl to be present on your system.

On Mac this is a bit awkward, but it can be installed like this:

```brew install openssl```

Unfortunately, the libraries still aren't in the right path to be found, so you still have to do the following so they can be found:

```
# These paths might not be exactly the same on your system, but can be found by running `brew list openssl` to list the files installed.
export LDFLAGS="-L/opt/homebrew/Cellar/openssl@3/3.3.1/lib"
export CPPFLAGS="-I/opt/homebrew/Cellar/opennsl@3/3.3.1/include"
```

And then *hopefully* it just works (you might need to install libpq too with homebrew but I already had it.)
