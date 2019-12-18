package = 'indexpiration'
version = 'scm-1'
source  = {
    url    = 'git://github.com/moonlibs/indexpiration.git',
    branch = 'master',
}
description = {
    summary  = "Expire space records using index",
    homepage = 'https://github.com/moonlibs/indexpiration.git',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',
    modules = {
        ['indexpiration'] = 'indexpiration.lua'
    }
}

-- vim: syntax=lua
