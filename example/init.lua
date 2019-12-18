box.cfg{
}
box.once('access:v1', function()
	box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

clock = require 'clock'

require 'strict'.on()

require 'package.reload'

local format = {
		[1] = {name = "id", type = "string"},
		[2] = {name = "status", type = "string"},
		[3] = {name = "deadline", type = "number"},
		[4] = {name = "other", type = "number"},
	}
local _,e = box.schema.space.create('to_expire', {
	format = format,
	if_not_exists = true
})
if e ~= 'created' then
	box.space.to_expire:format(format)
end

box.space.to_expire:create_index('primary', { unique = true, parts = {1, 'str'}, if_not_exists = true})
box.space.to_expire:create_index('exp', { unique = false, parts = { 3, 'number', 1, 'str' }, if_not_exists = true})

if not package.path:match('%.%./%?%.lua;') then
	package.path = '../?.lua;' .. package.path
end

require 'indexpiration'( box.space.to_expire, {
	-- field = 3;
	field = 'deadline';
	-- kind = 'time64';
	kind = 'time';
	
	-- kind  = function(t)
	-- 	-- print("Examine",t, tonumber(t.deadline - clock.realtime()))
	-- 	return tonumber(t.deadline - clock.realtime())
	-- end,
	
	debug = true;
	precise = true; -- sub-second precision when waiting...
	on_delete = function(t)
		-- print("Tuple is about to delete",t,clock.time() - t.deadline)
	end
} )

box.space.to_expire:insert({'O','will not be deleted',0,0})

for i =1,100 do box.space.to_expire:insert({tostring(i),'xxx',clock.time()+1.1,0}) end

pcall(function() require'console'.start() os.exit() end)

