local M = {}

local ffi = require 'ffi'
local log = require 'log'
local fiber = require 'fiber'
local clock = require 'clock'

local function table_clear(t)
	if type(t) ~= 'table' then
		error("bad argument #1 to 'clear' (table expected, got "..(t ~= nil and type(t) or 'no value')..")",2)
	end
	local count = #t
	for i=0, count do t[i]=nil end
	return
end

local json = require 'json'.new()
json.cfg{ encode_invalid_as_nil = true }
local yaml = require 'yaml'
local function dd(x)
	print(yaml.encode(x))
end


local function typeeq(src, ref)
	if ref == 'str' then
		return src == 'STR' or src == 'str' or src == 'string'
	elseif ref == 'num' then
		return src == 'NUM' or src == 'num' or src == 'number' or src == 'unsigned'
	else
		return src == ref
	end
end

local function _mk_keyfunc(index)
	local fun
	if #index.parts == 1 then
		fun = ('return function (t) return t and t[%s] or nil end'):format( index.parts[1].fieldno )
	else
		local rows = {}
		for k = 1,#index.parts do
			table.insert(rows,("\tt[%s]==nil and NULL or t[%s],\n")
				:format(index.parts[k].fieldno,index.parts[k].fieldno))
		end
		fun = "local NULL = require'msgpack'.NULL return function(t) return "..
			"t and {\n"..table.concat(rows, "").."} or nil end\n"
	end
	-- print(fun)
	return dostring(fun)
end

local function _callable(f)
	if type(f) == 'function' then
		return true
	else
		local mt = debug.getmetatable(f)
		return mt and mt.__call
	end
end

local F = {}

function F:stop()
	self.stopped = true
	self._wait:put(true,0)
end

function F:start_worker()
	self._worker = fiber.create(function(space,expiration,expire_index)
		local fname = space.name .. '.xpr'
		if package.reload then fname = fname .. '.' .. package.reload.count end
		fiber.name(string.sub(fname,1,32))
		repeat fiber.sleep(0.001) until space.expiration
		local chan = expiration._wait
		log.info("Worker started")
		local curwait
		local collect = {}
		while box.space[space.name] and space.expiration == expiration and not expiration.stopped do
			local r,e = pcall(function()
				-- print("runat loop 2 ",box.time64())
				local remaining
				for _,t in expire_index:pairs({0},{iterator = box.index.GT}) do
					-- print("checking ",t)
					local delta = expiration.check( t )
					
					if delta <= 0 then
						table.insert(collect,t)
					else
						remaining = delta
						break
					end
					
					if #collect >= expiration.batch_size then
						remaining = 0
						break
					end
				end
				
				if next(collect) then
					-- print("batch collected", #collect)
					if expiration.txn then
						box.begin()
					end
					for _,t in pairs(collect) do
						if not expiration.txn then
							t = box.space[space.name]:get(expiration._pk(t))
							if expiration.check( t ) > 0 then
								t = nil
							end
						end
						if t then
							local skip = false
							if expiration.on_delete then
								skip = false == expiration.on_delete(t)
							end
							if not skip then
								space:delete( expiration._pk(t) )
							end
						end
					end
					if expiration.txn then
						box.commit()
					end
					-- print("batch deleted")
				end
				
				if remaining then
					if remaining >= 0 and remaining < 1 then
						return remaining
					end
				end
				return 1
			end)
			
			table_clear(collect)

			if r then
				curwait = e
			else
				curwait = 1
				log.error("Worker/ERR: %s",e)
			end
			-- log.info("Wait %0.2fs",curwait)
			if curwait == 0 then fiber.sleep(0) end
			chan:get(curwait)
		end
		if expiration.stopped then
			log.info("Expiration worker was stopped")
		else
			log.info("Worker finished")
		end
	end,box.space[self.space],self,self.expire_index)
end

function M.upgrade(space,opts,depth)
	depth = depth or 0
	log.info("Indexpiration upgrade(%s,%s)", space.name, json.encode(opts))
	if not opts.field then error("opts.field required",2) end
	
	local self = setmetatable({},{ __index = F })
	if space.expiration then
		self._wait = space.expiration._wait
		self._stat = space.expiration._stat
	else
		self._wait = fiber.channel(0)
	end
	self.debug = not not opts.debug
	if opts.txn ~= nil then
		self.txn = not not opts.txn
	else
		self.txn = true
	end
	self.on_delete = opts.on_delete
	self.precise = not not self.precise
	self.batch_size = opts.batch_size or 300
		
	local format_av = box.space._space.index.name:get(space.name)[ 7 ]
	local format = {}
	local have_format = false
	for no,f in pairs(format_av) do
		format[ f.name ] = {
			name = f.name;
			type = f.type;
			no   = no;
		}
		format[ no ] = format[ f.name ];
		have_format = true
	end
	for _,idx in pairs(space.index) do
		for _,part in pairs(idx.parts) do
			format[ part.fieldno ] = format[ part.fieldno ] or { no = part.fieldno }
			format[ part.fieldno ].type = part.type
		end
	end

	-- dd(format)

	local expire_field_no
	if have_format then
		expire_field_no = format[ opts.field ].no
	else
		expire_field_no = opts.field
	end
	if type(expire_field_no) ~= 'number' then
		error("Need corrent `.field` option", 2+depth)
	end
	
	-- 2. index check
	local expire_index
	local has_non_tree
	for _,index in pairs(space.index) do
		if index.parts[1].fieldno == expire_field_no then
			if index.type == 'TREE' then
				expire_index = index
				break
			else
				has_non_tree = index
			end
		end
	end
	
	if not expire_index then
		if has_non_tree then
			error(string.format("index %s must be TREE (or create another)", has_non_tree.name),2+depth)
		else
			error(string.format("field %s requires tree index with it as first field", opts.field),2+depth)
		end
	end

	self._pk = _mk_keyfunc(space.index[0])
	
	-- if not self._stat then
	-- 	self._stat = {
	-- 		counts = {};
	-- 	}
	-- end
	
	if opts.kind == 'time' or opts.kind == 'time64' then
		if not typeeq(expire_index.parts[1].type,'num') then
			error(("Can't use field %s as %s"):format(opts.field,opts.kind),2+depth)
		end
		if opts.kind == 'time' then
			self.check = function(t)
				return t[ expire_field_no ] - clock.realtime()
			end
		elseif opts.kind == 'time64' then
			self.check = function(t)
				return tonumber(
					ffi.cast('int64_t',t[ expire_field_no ])
					- ffi.cast('int64_t',clock.realtime64())
				)/1e9
			end
		end
	elseif _callable(opts.kind) then
		self.check = opts.kind
	else
		error(("Unsupported kind: %s"):format(opts.kind),2+depth)
	end

	self.space = space.id
	self.expire_index = expire_index

	self:start_worker()
	

	if self.precise then
		self._on_repl = space:on_replace(function(old, new)
			self._wait:put(true,0)
		end, self._on_repl)
	end
	rawset(space,'expiration',self)
	
	while self._wait:has_readers() do
		self._wait:put(true,0)
	end

	log.info("Upgraded %s into indexpiration (status=%s)", space.name, box.info.status)
end

setmetatable(M,{
	__call = function(self, space, opts)
		self.upgrade(space,opts,1)
	end
})

return M
