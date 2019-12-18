# indexpiration
## Expire space records using index

```lua
local indexpiration = require 'indexpiration'

indexpiration.upgrade(box.space.myspace, {
    field = number | 'name',
    kind  = 'time' | 'time64',
    
    -- optional parameters
    txn   = false, -- disable transaction on batch deletion
    batch_size = 1000, -- change batch size (default 300)
    on_delete = function(tuple)
        -- handler called on tuple just before delete
        -- must not yield if txn is used
    end,
    precise = true,
})

box.space.myspace.expiration:stop() -- stop expiration fiber

-- ## Alternative check function

indexpiration.upgrade(box.space.myspace, {
    field = number | 'name',
    kind  = function(t)
        -- calculate time, left for tuple to live and return it.
        -- time MUST correlate with order in index for `field`
        
        -- this is sample of 'time64' kind (beware of unsigneds):
        return tonumber(
            ffi.cast('int64_t',t[ expire_field_no ])
            - ffi.cast('int64_t',clock.realtime64())
        )/1e9
    end,
})

-- Not implemented yet:
-- box.space.myspace.expiration:stat()

-- box.space.myspace.expiration:pause()
-- box.space.myspace.expiration:resume()

```
