set mongod="C:\Program Files\MongoDB\Server\3.0\bin\mongod.exe"
if not exist %mongod% set mongod="C:\Program Files (x86)\MongoDB\Server\3.0\bin\mongod.exe"
if not exist %mongod% set mongod="C:\Program Files\MongoDB\Server\3.1\bin\mongod.exe"
if not exist %mongod% set mongod="C:\Program Files (x86)\MongoDB\Server\3.1\bin\mongod.exe"
if not exist %mongod% set mongod="C:\Program Files\MongoDB\Server\3.2\bin\mongod.exe"
if not exist %mongod% set mongod="C:\Program Files (x86)\MongoDB\Server\3.2\bin\mongod.exe"

set cur_path=%~dp0
set dbpath="%cur_path%mongodata"

%mongod% --port 27017 --dbpath=%dbpath% --nojournal
