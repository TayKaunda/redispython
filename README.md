# A simple redis clone written in python
The server  will be able to respond to the following commands:

GET <key>
SET <key> <value>
DELETE <key>
FLUSH
MGET <key1> ... <keyn>
MSET <key1> <value1> ... <keyn> <valuen>
We'll support the following data-types as well:

Strings and Binary Data
Numbers
NULL
Arrays (which may be nested)
Dictionaries (which may be nested)
Error messages
To handle multiple clients asynchronously, we'll be  using gevent,
