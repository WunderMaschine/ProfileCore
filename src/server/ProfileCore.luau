local CONFIG = {
	AutoSaveCycle = 30,
	APICooldown = 7,
	ForceAcquireLimit = 8,
	SessionDeadTime = 1800,
	IssuesForEmergency = 5,
	IssueExpiration = 120,
	EmergencyDuration = 120,
	MetaSyncKeys = {
		ProfileCreateTime = true,
		SessionLoadCount = true,
		ActiveSession = true,
		ForceLoadSession = true,
		LastUpdate = true,
	},
}

local Util do
	local SignalCore = {}
	local FreeThread = nil
	local function RunHandler(fn, ...)
		local thread = FreeThread
		FreeThread = nil
		fn(...)
		FreeThread = thread
	end
	local function HandlerLoop(...)
		RunHandler(...)
		while true do
			RunHandler(coroutine.yield())
		end
	end
	local Connection = {}
	Connection.__index = Connection
	function Connection:Disconnect()
		if not self._connected then
			return
		end
		self._connected = false
		self._signal._count -= 1
		if self._signal._head == self then
			self._signal._head = self._next
		else
			local p = self._signal._head
			while p and p._next ~= self do
				p = p._next
			end
			if p then
				p._next = self._next
			end
		end
		if self._onDisc then
			if not FreeThread then
				FreeThread = coroutine.create(HandlerLoop)
			end
			task.spawn(FreeThread, self._onDisc, self._param)
			self._onDisc = nil
		end
	end
	local Signal = {}
	Signal.__index = Signal
	function Signal:Connect(fn, dfn, prm)
		local c = {
			_listener = fn,
			_signal = self,
			_onDisc = dfn,
			_param = prm,
			_next = self._head,
			_connected = true,
		}
		setmetatable(c, Connection)
		self._head = c
		self._count += 1
		return c
	end
	function Signal:GetListenerCount()
		return self._count
	end
	function Signal:Fire(...)
		local n = self._head
		while n do
			if n._connected then
				if not FreeThread then
					FreeThread = coroutine.create(HandlerLoop)
				end
				task.spawn(FreeThread, n._listener, ...)
			end
			n = n._next
		end
	end
	function Signal:FireUntil(check, ...)
		local n = self._head
		while n do
			if n._connected then
				n._listener(...)
				if check() ~= true then
					return
				end
			end
			n = n._next
		end
	end
	function SignalCore.NewSignal()
		return {
			_head = nil,
			_count = 0,
			Connect = Signal.Connect,
			GetListenerCount = Signal.GetListenerCount,
			Fire = Signal.Fire,
			FireUntil = Signal.FireUntil,
		}
	end
	Util = {
		NewSignal = SignalCore.NewSignal,
		OnShutdown = function(taskf, runstudio)
			if game:GetService("RunService"):IsStudio() == false or runstudio == true then
				game:BindToClose(taskf)
			end
		end,
	}
end

local DataEngine = {
	SystemHalted = false,
	ErrorEvent = Util.NewSignal(),
	DamageEvent = Util.NewSignal(),
	EmergencyMode = false,
	EmergencySignal = Util.NewSignal(),
	SystemIssueCount = 0,
	_active_stores = {},
	_auto_saves = {},
	_issues = {},
	_emergency_start = 0,
	_mock_main = {},
	_mock_user = {},
	_mock_flag = false,
}

local ActiveStores = DataEngine._active_stores
local AutoSaves = DataEngine._auto_saves
local Issues = DataEngine._issues
local DSService = game:GetService("DataStoreService")
local RunService = game:GetService("RunService")
local CurrentPlace = game.PlaceId
local CurrentJob = game.JobId
local AutoIndex = 1
local LastSave = os.clock()
local LoadCounter = 0
local ActiveLoads = 0
local ActiveSaves = 0
local EmergencyStart = 0
local IsStudio = RunService:IsStudio()
local LiveCheck = false
local UseMock = false
local MockMain = DataEngine._mock_main
local MockUser = DataEngine._mock_user
local MockTag = {}
local WriteQueue = {}

local function DeepCopy(o)
	local c = {}
	for k,v in pairs(o) do
		if type(v) == "table" then
			c[k] = DeepCopy(v)
		else
			c[k] = v
		end
	end
	return c
end

local function MergeData(tgt,tmpl)
	for a,b in pairs(tmpl) do
		if type(a)=="string" then
			if tgt[a]==nil then
				if type(b)=="table" then
					tgt[a]=DeepCopy(b)
				else
					tgt[a]=b
				end
			elseif type(tgt[a])=="table" and type(b)=="table" then
				MergeData(tgt[a],b)
			end
		end
	end
end

local function IdentifyStore(n,s,k)
	return string.format('[Store:"%s";%sKey:"%s"]',n,s and ('Scope:"'..s..'";') or "",k)
end

local function CleanupWriteQueue(st, ky)
	if WriteQueue[st] then
		WriteQueue[st][ky]=nil
		if next(WriteQueue[st])==nil then
			WriteQueue[st]=nil
		end
	end
end

local function MarkWriteCleanup(st, ky)
	if WriteQueue[st] then
		if WriteQueue[st][ky] then
			local q=WriteQueue[st][ky]
			local qu=q.Queue
			if q.CleanupJob==nil then
				q.CleanupJob=RunService.Heartbeat:Connect(function()
					if os.clock()-q.LastWrite>CONFIG.APICooldown and #qu==0 then
						q.CleanupJob:Disconnect()
						CleanupWriteQueue(st,ky)
					end
				end)
			end
		elseif next(WriteQueue[st])==nil then
			WriteQueue[st]=nil
		end
	end
end

local function QueueAsync(cb, st, ky)
	if not WriteQueue[st] then
		WriteQueue[st]={}
	end
	if not WriteQueue[st][ky] then
		WriteQueue[st][ky]={LastWrite=0,Queue={},CleanupJob=nil}
	end
	local qd=WriteQueue[st][ky]
	local q=qd.Queue
	if os.clock()-qd.LastWrite>CONFIG.APICooldown and #q==0 then
		qd.LastWrite=os.clock()
		return cb()
	else
		table.insert(q,cb)
		while true do
			if os.clock()-qd.LastWrite>CONFIG.APICooldown and q[1]==cb then
				table.remove(q,1)
				qd.LastWrite=os.clock()
				return cb()
			end
			task.wait()
		end
	end
end

local function QueueEmpty(st,ky)
	local l=WriteQueue[st]
	if l then
		l=l[ky]
		return l==nil or #l.Queue==0
	end
	return true
end

local function WaitLive()
	while LiveCheck do
		task.wait()
	end
end

local function WaitStore(pst)
	while pst._is_pending do
		task.wait()
	end
end

local function ReportIssue(msg, sn, sc, pk)
	warn("[DataEngine]: DataStore API error "..IdentifyStore(sn,sc,pk)..' - "'..tostring(msg)..'"')
	table.insert(Issues,os.clock())
	DataEngine.ErrorEvent:Fire(tostring(msg), sn, pk)
end

local function ReportDamage(sn,sc,pk)
	warn("[DataEngine]: Resolved data corruption "..IdentifyStore(sn,sc,pk))
	DataEngine.DamageEvent:Fire(sn,pk)
end

local function NewMockKeyInfo(p)
	local vs = tostring(p.VersionId or 0)
	local md = p.MetaData or {}
	local us = p.UserIds or {}
	return {
		CreatedTime = p.CreatedTime,
		UpdatedTime = p.UpdatedTime,
		Version = string.rep("0",16).."."..string.rep("0",10-#vs)..vs.."."..string.rep("0",16)..".".."01",
		GetMetadata = function()
			return DeepCopy(md)
		end,
		GetUserIds = function()
			return DeepCopy(us)
		end,
	}
end

local function MockTransform(md, nm, k, func, get)
	local st = md[nm]
	if not st then
		st={}
		md[nm]=st
	end
	local epoch = math.floor(os.time()*1000)
	local en = st[k]
	local wasnil = false
	if not en and get~=true then
		wasnil=true
		en={Data=nil,CreatedTime=epoch,UpdatedTime=epoch,VersionId=0,UserIds={},MetaData={}}
		st[k]=en
	end
	local ki = not wasnil and NewMockKeyInfo(en) or nil
	local tr,uid,rm = func(en and en.Data, ki)
	if not tr then
		return nil
	else
		if en and get~=true then
			en.Data = tr
			en.UserIds = DeepCopy(uid or {})
			en.MetaData = DeepCopy(rm or {})
			en.VersionId += 1
			en.UpdatedTime=epoch
		end
		return DeepCopy(tr), en and NewMockKeyInfo(en) or nil
	end
end

local function MockWrapUser(nm, k, f, g)
	return MockTransform(MockUser, nm, k, f, g)
end
local function MockWrapMain(nm, k, f, g)
	return MockTransform(MockMain, nm, k, f, g)
end

local function ThisSession(sess)
	return sess[1]==CurrentPlace and sess[2]==CurrentJob
end

local function RunUpdate(store, key, ops, usr, isget, ver)
	local dat,info
	local s,e = pcall(function()
		local xf = function(ld)
			local miss=false
			local corrupt=false
			local gd={0,{}}
			if not ld then
				miss=true
			elseif type(ld)~="table" then
				miss=true
				corrupt=true
			end
			if type(ld)=="table" then
				if type(ld.Data)=="table" and type(ld.MetaData)=="table" and type(ld.GlobalUpdates)=="table" then
					ld.WasCorrupted=false
					gd=ld.GlobalUpdates
					if ops.ExistingProfileHandle then
						ops.ExistingProfileHandle(ld)
					end
				elseif ld.Data==nil and ld.MetaData==nil and type(ld.GlobalUpdates)=="table" then
					ld.WasCorrupted=false
					gd=ld.GlobalUpdates or gd
					miss=true
				else
					miss=true
					corrupt=true
				end
			end
			if miss then
				ld={GlobalUpdates=gd}
				if ops.MissingProfileHandle then
					ops.MissingProfileHandle(ld)
				end
			end
			if ops.EditProfile then
				ops.EditProfile(ld)
			end
			if corrupt then
				ld.WasCorrupted=true
			end
			return ld, ld.UserIds, ld.RobloxMetaData
		end
		if usr==true then
			dat,info=MockWrapUser(store._profile_store_lookup,key,xf,isget)
			task.wait()
		elseif UseMock then
			dat,info=MockWrapMain(store._profile_store_lookup,key,xf,isget)
			task.wait()
		else
			dat,info=QueueAsync(function()
				if isget==true then
					local gd,ki
					if ver then
						local ss,em=pcall(function()
							gd,ki=store._global_data_store:GetVersionAsync(key,ver)
						end)
						if ss==false and type(em)=="string" and string.find(em,"not valid") then
							warn("[DataEngine]: Passed version argument is not valid; Traceback:\n"..debug.traceback())
						end
					else
						gd,ki=store._global_data_store:GetAsync(key)
					end
					gd=xf(gd)
					return gd,ki
				else
					return store._global_data_store:UpdateAsync(key,xf)
				end
			end, store._profile_store_lookup, key)
		end
	end)
	if s and type(dat)=="table" then
		if dat.WasCorrupted==true and isget~=true then
			ReportDamage(store._profile_store_name,store._profile_store_scope,key)
		end
		return dat,info
	else
		ReportIssue(e~=nil and e or "Undefined error",store._profile_store_name,store._profile_store_scope,key)
		return nil
	end
end

local function RemoveAutoSave(obj)
	local i=table.find(AutoSaves,obj)
	if i then
		table.remove(AutoSaves,i)
		if i<AutoIndex then
			AutoIndex=AutoIndex-1
		end
		if not AutoSaves[AutoIndex] then
			AutoIndex=1
		end
	end
end

local function AddAutoSave(obj)
	table.insert(AutoSaves,AutoIndex,obj)
	if #AutoSaves>1 then
		AutoIndex=AutoIndex+1
	elseif #AutoSaves==1 then
		LastSave=os.clock()
	end
end

local function InternalRelease(obj)
	local st=obj._record_store
	local lp = obj._is_user_mock and st._mock_loaded_profiles or st._loaded_profiles
	lp[obj._record_key]=nil
	if next(st._loaded_profiles)==nil and next(st._mock_loaded_profiles)==nil then
		local idx=table.find(ActiveStores,st)
		if idx then
			table.remove(ActiveStores,idx)
		end
	end
	RemoveAutoSave(obj)
	local pid
	local jid
	local as=obj.MetaData.ActiveSession
	if as then
		pid=as[1]
		jid=as[2]
	end
	obj._release_listeners:Fire(pid,jid)
end

local function CheckGlobalChanges(obj,old,new)
	local gu=obj.GlobalUpdates
	local pl=gu._pending_update_lock
	local pc=gu._pending_update_clear
	for _,nw in ipairs(new[2]) do
		local oldgu
		for _,g in ipairs(old[2]) do
			if g[1]==nw[1] then
				oldgu=g
				break
			end
		end
		local fresh=false
		if not oldgu or nw[2]>oldgu[2] or nw[3]~=oldgu[3] then
			fresh=true
		end
		if fresh then
			if nw[3]==false then
				local pending_l=false
				for _,lid in ipairs(pl) do
					if nw[1]==lid then
						pending_l=true
						break
					end
				end
				if not pending_l then
					gu._new_active_update_listeners:Fire(nw[1],nw[4])
				end
			end
			if nw[3]==true then
				local pending_c=false
				for _,cid in ipairs(pc) do
					if nw[1]==cid then
						pending_c=true
						break
					end
				end
				if not pending_c then
					gu._new_locked_update_listeners:FireUntil(function()
						return table.find(pc,nw[1])==nil
					end,nw[1],nw[4])
				end
			end
		end
	end
end

local function CommitSave(obj,release_flag,overwrite)
	if type(obj.Data)~="table" then
		ReportDamage(obj._record_store._profile_store_name,obj._record_store._profile_store_scope,obj._record_key)
		error("[DataEngine]: DATA CORRUPTED RUNTIME! "..obj:Identify())
	end
	if release_flag==true and overwrite~=true then
		InternalRelease(obj)
	end
	ActiveSaves+=1
	local last_load=obj.MetaData.SessionLoadCount
	local again=true
	while again do
		if release_flag~=true then
			again=false
		end
		local ld,ki=RunUpdate(
			obj._record_store,
			obj._record_key,
			{
				ExistingProfileHandle=nil,
				MissingProfileHandle=nil,
				EditProfile=function(now)
					local sess_own=false
					local force_pend=false
					if overwrite~=true then
						local ac=now.MetaData.ActiveSession
						local fl=now.MetaData.ForceLoadSession
						local slc=now.MetaData.SessionLoadCount
						if type(ac)=="table" then
							sess_own=ThisSession(ac) and slc==last_load
						end
						if type(fl)=="table" then
							force_pend=not ThisSession(fl)
						end
					else
						sess_own=true
					end
					if sess_own then
						if overwrite~=true then
							local gu_data=now.GlobalUpdates
							local gu_list=gu_data[2]
							local gu_obj=obj.GlobalUpdates
							local pl=gu_obj._pending_update_lock
							local pc=gu_obj._pending_update_clear
							for i=1,#gu_list do
								for _,lk in ipairs(pl) do
									if gu_list[i][1]==lk then
										gu_list[i][3]=true
										break
									end
								end
							end
							for _,cl in ipairs(pc) do
								for i2=1,#gu_list do
									if gu_list[i2][1]==cl and gu_list[i2][3]==true then
										table.remove(gu_list,i2)
										break
									end
								end
							end
						end
						now.Data=obj.Data
						now.RobloxMetaData=obj.RobloxMetaData
						now.UserIds=obj.UserIds
						if overwrite~=true then
							now.MetaData.MetaTags=obj.MetaData.MetaTags
							now.MetaData.LastUpdate=os.time()
							if release_flag==true or force_pend==true then
								now.MetaData.ActiveSession=nil
							end
						else
							now.MetaData=obj.MetaData
							now.MetaData.ActiveSession=nil
							now.MetaData.ForceLoadSession=nil
							now.GlobalUpdates=obj.GlobalUpdates._updates_latest
						end
					end
				end,
			},
			obj._is_user_mock
		)
		if ld and ki then
			if overwrite==true then
				break
			end
			again=false
			obj.KeyInfo=ki
			local gu_obj=obj.GlobalUpdates
			local oldg=gu_obj._updates_latest
			local newg=ld.GlobalUpdates
			gu_obj._updates_latest=newg
			local smd=obj.MetaData
			local lmd=ld.MetaData
			for ky in pairs(CONFIG.MetaSyncKeys) do
				smd[ky]=lmd[ky]
			end
			smd.MetaTagsLatest=lmd.MetaTags
			local as=ld.MetaData.ActiveSession
			local slc=ld.MetaData.SessionLoadCount
			local sess_own=false
			if type(as)=="table" then
				sess_own=ThisSession(as) and slc==last_load
			end
			local act=obj:IsActive()
			if sess_own then
				if act then
					CheckGlobalChanges(obj,oldg,newg)
				end
			else
				if act then
					InternalRelease(obj)
				end
				MarkWriteCleanup(obj._record_store._profile_store_lookup,obj._record_key)
				if not obj._hop_ready then
					obj._hop_ready=true
					obj._hop_ready_listeners:Fire()
				end
			end
			obj.MetaTagsUpdated:Fire(obj.MetaData.MetaTagsLatest)
			obj.KeyInfoUpdated:Fire(ki)
		elseif again then
			task.wait()
		end
	end
	ActiveSaves-=1
end

local GUpdates={}
GUpdates.__index=GUpdates

function GUpdates:GetActiveUpdates()
	local r={}
	for _,g in ipairs(self._updates_latest[2]) do
		if g[3]==false then
			local pl=self._pending_update_lock
			local skip=false
			if pl then
				for _,id in ipairs(pl) do
					if g[1]==id then
						skip=true
						break
					end
				end
			end
			if not skip then
				table.insert(r,{g[1],g[4]})
			end
		end
	end
	return r
end

function GUpdates:GetLockedUpdates()
	local r={}
	for _,g in ipairs(self._updates_latest[2]) do
		if g[3]==true then
			local pc=self._pending_update_clear
			local skip=false
			if pc then
				for _,id in ipairs(pc) do
					if g[1]==id then
						skip=true
						break
					end
				end
			end
			if not skip then
				table.insert(r,{g[1],g[4]})
			end
		end
	end
	return r
end

function GUpdates:ListenToNewActiveUpdate(fn)
	if type(fn)~="function" then
		error("[DataEngine]: Invalid listener")
	end
	local pr=self._record
	if self._update_handler_mode==true then
		error("[DataEngine]: Cannot listen in update mode")
	elseif self._new_active_update_listeners==nil then
		error("[DataEngine]: Cannot listen in view mode")
	elseif not pr:IsActive() then
		return {Disconnect=function()end}
	end
	return self._new_active_update_listeners:Connect(fn)
end

function GUpdates:ListenToNewLockedUpdate(fn)
	if type(fn)~="function" then
		error("[DataEngine]: Invalid listener")
	end
	local pr=self._record
	if self._update_handler_mode==true then
		error("[DataEngine]: Cannot listen in update mode")
	elseif self._new_locked_update_listeners==nil then
		error("[DataEngine]: Cannot listen in view mode")
	elseif not pr:IsActive() then
		return {Disconnect=function()end}
	end
	return self._new_locked_update_listeners:Connect(fn)
end

function GUpdates:LockActiveUpdate(uid)
	if type(uid)~="number" then
		error("[DataEngine]: Invalid update_id")
	end
	local pr=self._record
	if self._update_handler_mode==true then
		error("[DataEngine]: Cannot lock in update mode")
	elseif self._pending_update_lock==nil then
		error("[DataEngine]: Cannot lock in view mode")
	elseif not pr:IsActive() then
		error("[DataEngine]: EXPIRED")
	end
	local found=nil
	for _,u in ipairs(self._updates_latest[2]) do
		if u[1]==uid then
			found=u
			break
		end
	end
	if found then
		local dup=false
		for _,x in ipairs(self._pending_update_lock) do
			if uid==x then
				dup=true
				break
			end
		end
		if not dup and found[3]==false then
			table.insert(self._pending_update_lock,uid)
		end
	else
		error("[DataEngine]: Non-existent update_id")
	end
end

function GUpdates:ClearLockedUpdate(uid)
	if type(uid)~="number" then
		error("[DataEngine]: Invalid update_id")
	end
	local pr=self._record
	if self._update_handler_mode==true then
		error("[DataEngine]: Cannot clear in update mode")
	elseif self._pending_update_clear==nil then
		error("[DataEngine]: Cannot clear in view mode")
	elseif not pr:IsActive() then
		error("[DataEngine]: EXPIRED")
	end
	local found=nil
	for _,u in ipairs(self._updates_latest[2]) do
		if u[1]==uid then
			found=u
			break
		end
	end
	if found then
		local dup=false
		for _,x in ipairs(self._pending_update_clear) do
			if uid==x then
				dup=true
				break
			end
		end
		if not dup and found[3]==true then
			table.insert(self._pending_update_clear,uid)
		end
	else
		error("[DataEngine]: Non-existent update_id")
	end
end

function GUpdates:AddActiveUpdate(d)
	if type(d)~="table" then
		error("[DataEngine]: Invalid update_data")
	end
	if self._new_active_update_listeners~=nil then
		error("[DataEngine]: Cannot add active updates in loaded record; use GlobalUpdate")
	elseif self._update_handler_mode~=true then
		error("[DataEngine]: Cannot add in view mode")
	end
	local ul=self._updates_latest
	local idx=ul[1]+1
	ul[1]=idx
	table.insert(ul[2],{idx,1,false,d})
end

function GUpdates:ChangeActiveUpdate(uid,d)
	if type(uid)~="number" then
		error("[DataEngine]: Invalid update_id")
	end
	if type(d)~="table" then
		error("[DataEngine]: Invalid update_data")
	end
	if self._new_active_update_listeners~=nil then
		error("[DataEngine]: Cannot change active updates in loaded record; use GlobalUpdate")
	elseif self._update_handler_mode~=true then
		error("[DataEngine]: Cannot change in view mode")
	end
	local ul=self._updates_latest
	local found=nil
	for _,g in ipairs(ul[2]) do
		if g[1]==uid then
			found=g
			break
		end
	end
	if found then
		if found[3]==true then
			error("[DataEngine]: Cannot change locked")
		end
		found[2]=found[2]+1
		found[4]=d
	else
		error("[DataEngine]: Non-existent update_id")
	end
end

function GUpdates:ClearActiveUpdate(uid)
	if type(uid)~="number" then
		error("[DataEngine]: Invalid update_id")
	end
	if self._new_active_update_listeners~=nil then
		error("[DataEngine]: Cannot clear active in loaded record; use GlobalUpdate")
	elseif self._update_handler_mode~=true then
		error("[DataEngine]: Cannot clear in view mode")
	end
	local ul=self._updates_latest
	local fidx=nil
	local found=nil
	for i,g in ipairs(ul[2]) do
		if g[1]==uid then
			fidx=i
			found=g
			break
		end
	end
	if found then
		if found[3]==true then
			error("[DataEngine]: Cannot clear locked")
		end
		table.remove(ul[2],fidx)
	else
		error("[DataEngine]: Non-existent update_id")
	end
end

local Record={}
Record.__index=Record

function Record:IsActive()
	local lps=self._is_user_mock and self._record_store._mock_loaded_profiles or self._record_store._loaded_profiles
	return lps[self._record_key]==self
end

function Record:GetMetaTag(tn)
	local m=self.MetaData
	if not m then
		return nil
	end
	return self.MetaData.MetaTags[tn]
end

function Record:SetMetaTag(tn,v)
	if type(tn)~="string" or #tn==0 then
		error("[DataEngine]: Invalid tag_name")
	end
	self.MetaData.MetaTags[tn]=v
end

function Record:Reconcile()
	MergeData(self.Data,self._record_store._profile_template)
end

function Record:ListenToRelease(fn)
	if type(fn)~="function" then
		error("[DataEngine]: Invalid listener")
	end
	if self._view_mode then
		return {Disconnect=function()end}
	end
	if not self:IsActive() then
		local pid,jid
		local as=self.MetaData.ActiveSession
		if as then
			pid=as[1]
			jid=as[2]
		end
		fn(pid,jid)
		return {Disconnect=function()end}
	else
		return self._release_listeners:Connect(fn)
	end
end

function Record:Save()
	if self._view_mode then
		error("[DataEngine]: Cannot save in view mode")
	end
	if not self:IsActive() then
		warn("[DataEngine]: Saving inactive record "..self:Identify()..";\n"..debug.traceback())
		return
	end
	if QueueEmpty(self._record_store._profile_store_lookup,self._record_key) then
		RemoveAutoSave(self)
		AddAutoSave(self)
		task.spawn(CommitSave,self)
	end
end

function Record:Release()
	if self._view_mode then
		return
	end
	if self:IsActive() then
		task.spawn(CommitSave,self,true)
	end
end

function Record:ListenToHopReady(fn)
	if type(fn)~="function" then
		error("[DataEngine]: Invalid listener")
	end
	if self._view_mode then
		return {Disconnect=function()end}
	end
	if self._hop_ready then
		task.spawn(fn)
		return {Disconnect=function()end}
	else
		return self._hop_ready_listeners:Connect(fn)
	end
end

function Record:AddUserId(uid)
	if type(uid)~="number" or uid%1~=0 then
		warn("[DataEngine]: Invalid UserId ("..tostring(uid)..")\n"..debug.traceback())
		return
	end
	if uid<0 and self._is_user_mock~=true and not UseMock then
		return
	end
	if not table.find(self.UserIds,uid) then
		table.insert(self.UserIds,uid)
	end
end

function Record:RemoveUserId(uid)
	if type(uid)~="number" or uid%1~=0 then
		warn("[DataEngine]: Invalid UserId ("..tostring(uid)..")\n"..debug.traceback())
		return
	end
	local i=table.find(self.UserIds,uid)
	if i then
		table.remove(self.UserIds,i)
	end
end

function Record:Identify()
	return IdentifyStore(self._record_store._profile_store_name,self._record_store._profile_store_scope,self._record_key)
end

function Record:ClearGlobalUpdates()
	if not self._view_mode then
		error("[DataEngine]: ClearGlobalUpdates only in view mode")
	end
	local guo={_updates_latest={0,{}},_record=self}
	setmetatable(guo,GUpdates)
	self.GlobalUpdates=guo
end

function Record:OverwriteAsync()
	if not self._view_mode then
		error("[DataEngine]: OverwriteAsync only in view mode")
	end
	CommitSave(self,nil,true)
end

local VersionQuery={}
VersionQuery.__index=VersionQuery

function VersionQuery:_Proceed()
	while #self._queue>0 do
		local job=table.remove(self._queue,1)
		task.spawn(job)
		if self._yielded then
			break
		end
	end
end

function VersionQuery:NextAsync(stk)
	if not self._profile_store then
		return nil
	end
	local rec
	local done=false
	local function run()
		if self._fail then
			done=true
			return
		end
		if not self._pages then
			self._yielded=true
			task.spawn(function()
				rec=self:NextAsync(true)
				done=true
			end)
			local s,e=pcall(function()
				self._pages=self._profile_store._global_data_store:ListVersionsAsync(self._profile_key,self._sort_dir,self._min,self._max)
				self._idx=0
			end)
			if not s or not self._pages then
				warn("[DataEngine]: Version query fail - "..tostring(e))
				self._fail=true
			end
			self._yielded=false
			self:_Proceed()
			return
		end
		local pg=self._pages:GetCurrentPage()
		local nxt=pg[self._idx+1]
		if self._pages.IsFinished==true and not nxt then
			done=true
			return
		end
		if not nxt then
			self._yielded=true
			task.spawn(function()
				rec=self:NextAsync(true)
				done=true
			end)
			local su=pcall(function()
				self._pages:AdvanceToNextPageAsync()
				self._idx=0
			end)
			if not su or #self._pages:GetCurrentPage()==0 then
				self._fail=true
			end
			self._yielded=false
			self:_Proceed()
			return
		end
		self._idx+=1
		rec=self._profile_store:ViewProfileAsync(self._profile_key,nxt.Version)
		done=true
	end
	if not self._yielded then
		run()
	else
		if stk==true then
			table.insert(self._queue,1,run)
		else
			table.insert(self._queue,run)
		end
	end
	while not done do
		task.wait()
	end
	return rec
end

local RecordStorage={}
RecordStorage.__index=RecordStorage

function RecordStorage:LoadProfileAsync(pk,handler,um)
	handler=handler or "ForceLoad"
	if not self._profile_template then
		error("[DataEngine]: No profile template set")
	end
	if type(pk)~="string" or #pk==0 then
		error("[DataEngine]: Invalid profile_key")
	end
	if type(handler)~="function" and handler~="ForceLoad" and handler~="Steal" then
		error("[DataEngine]: Invalid handler")
	end
	if DataEngine.SystemHalted then
		return nil
	end
	WaitStore(self)
	local mock_user=um==MockTag
	for _,st in ipairs(ActiveStores) do
		if st._profile_store_lookup==self._profile_store_lookup then
			local lp=mock_user and st._mock_loaded_profiles or st._loaded_profiles
			if lp[pk] then
				error("[DataEngine]: Profile "..IdentifyStore(self._profile_store_name,self._profile_store_scope,pk).." already loaded")
			end
		end
	end
	ActiveLoads+=1
	local fl=handler=="ForceLoad"
	local steps=0
	local req=fl
	local steal=false
	local agr=handler=="Steal"
	while not DataEngine.SystemHalted do
		local jobs=mock_user and self._mock_profile_load_jobs or self._profile_load_jobs
		local ld,ki
		local lid=LoadCounter+1
		LoadCounter=lid
		local job=jobs[pk]
		if job then
			job[1]=lid
			while job[2]==nil do
				task.wait()
			end
			if job[1]==lid then
				ld,ki=table.unpack(job[2])
				jobs[pk]=nil
			else
				ActiveLoads-=1
				return nil
			end
		else
			job={lid,nil}
			jobs[pk]=job
			job[2]=table.pack(RunUpdate(
				self,pk,
				{
					ExistingProfileHandle=function(latest)
						if not DataEngine.SystemHalted then
							local as=latest.MetaData.ActiveSession
							local fls=latest.MetaData.ForceLoadSession
							if not as then
								latest.MetaData.ActiveSession={CurrentPlace,CurrentJob}
								latest.MetaData.ForceLoadSession=nil
							elseif type(as)=="table" then
								if not ThisSession(as) then
									local lu=latest.MetaData.LastUpdate
									if lu and os.time()-lu>CONFIG.SessionDeadTime then
										latest.MetaData.ActiveSession={CurrentPlace,CurrentJob}
										latest.MetaData.ForceLoadSession=nil
										return
									end
									if steal or agr then
										local fu=false
										if fls then
											fu=ThisSession(fls)
										end
										if fu or agr then
											latest.MetaData.ActiveSession={CurrentPlace,CurrentJob}
											latest.MetaData.ForceLoadSession=nil
										end
									elseif req then
										latest.MetaData.ForceLoadSession={CurrentPlace,CurrentJob}
									end
								else
									latest.MetaData.ForceLoadSession=nil
								end
							end
						end
					end,
					MissingProfileHandle=function(latest)
						latest.Data=DeepCopy(self._profile_template)
						latest.MetaData={
							ProfileCreateTime=os.time(),
							SessionLoadCount=0,
							ActiveSession={CurrentPlace,CurrentJob},
							ForceLoadSession=nil,
							MetaTags={},
						}
					end,
					EditProfile=function(latest)
						if not DataEngine.SystemHalted then
							local as=latest.MetaData.ActiveSession
							if as and ThisSession(as) then
								latest.MetaData.SessionLoadCount=latest.MetaData.SessionLoadCount+1
								latest.MetaData.LastUpdate=os.time()
							end
						end
					end,
				},
				mock_user
			))
			if job[1]==lid then
				ld,ki=table.unpack(job[2])
				jobs[pk]=nil
			else
				ActiveLoads-=1
				return nil
			end
		end
		if ld and ki then
			local as=ld.MetaData.ActiveSession
			if type(as)=="table" then
				if ThisSession(as) then
					ld.MetaData.MetaTagsLatest=DeepCopy(ld.MetaData.MetaTags)
					local guo={
						_updates_latest=ld.GlobalUpdates,
						_pending_update_lock={},
						_pending_update_clear={},
						_new_active_update_listeners=Util.NewSignal(),
						_new_locked_update_listeners=Util.NewSignal(),
						_record=nil,
					}
					setmetatable(guo,GUpdates)
					local rec={
						Data=ld.Data,
						MetaData=ld.MetaData,
						MetaTagsUpdated=Util.NewSignal(),
						RobloxMetaData=ld.RobloxMetaData or {},
						UserIds=ld.UserIds or {},
						KeyInfo=ki,
						KeyInfoUpdated=Util.NewSignal(),
						GlobalUpdates=guo,
						_record_store=self,
						_record_key=pk,
						_release_listeners=Util.NewSignal(),
						_hop_ready_listeners=Util.NewSignal(),
						_hop_ready=false,
						_load_timestamp=os.clock(),
						_is_user_mock=mock_user,
					}
					setmetatable(rec,Record)
					guo._record=rec
					if next(self._loaded_profiles)==nil and next(self._mock_loaded_profiles)==nil then
						table.insert(ActiveStores,self)
					end
					if mock_user then
						self._mock_loaded_profiles[pk]=rec
					else
						self._loaded_profiles[pk]=rec
					end
					AddAutoSave(rec)
					if DataEngine.SystemHalted then
						CommitSave(rec,true)
						rec=nil
					end
					ActiveLoads-=1
					return rec
				else
					if fl then
						local fls=ld.MetaData.ForceLoadSession
						local fu=false
						if fls then
							fu=ThisSession(fls)
						end
						if fu then
							if not req then
								steps=steps+1
								if steps==CONFIG.ForceAcquireLimit then
									steal=true
								end
							end
							task.wait()
						else
							ActiveLoads-=1
							return nil
						end
						req=false
					elseif agr then
						task.wait()
					else
						local h=handler(as[1],as[2])
						if h=="Repeat" then
							task.wait()
						elseif h=="Cancel" then
							ActiveLoads-=1
							return nil
						elseif h=="ForceLoad" then
							fl=true
							req=true
							task.wait()
						elseif h=="Steal" then
							agr=true
							task.wait()
						else
							error("[DataEngine]: Invalid handler return")
						end
					end
				end
			else
				ActiveLoads-=1
				return nil
			end
		else
			task.wait()
		end
	end
	ActiveLoads-=1
	return nil
end

function RecordStorage:GlobalUpdateProfileAsync(pk,fn,um)
	if type(pk)~="string" or #pk==0 then
		error("[DataEngine]: Invalid profile_key")
	end
	if type(fn)~="function" then
		error("[DataEngine]: Invalid update_handler")
	end
	if DataEngine.SystemHalted then
		return nil
	end
	WaitStore(self)
	while not DataEngine.SystemHalted do
		local ld=RunUpdate(
			self,pk,
			{
				ExistingProfileHandle=nil,
				MissingProfileHandle=nil,
				EditProfile=function(data)
					local guo={
						_updates_latest=data.GlobalUpdates,
						_update_handler_mode=true,
					}
					setmetatable(guo,GUpdates)
					fn(guo)
				end,
			},
			um==MockTag
		)
		MarkWriteCleanup(self._profile_store_lookup,pk)
		if ld then
			local guo={
				_updates_latest=ld.GlobalUpdates,
			}
			setmetatable(guo,GUpdates)
			return guo
		else
			task.wait()
		end
	end
	return nil
end

function RecordStorage:ViewProfileAsync(pk,ver,um)
	if type(pk)~="string" or #pk==0 then
		error("[DataEngine]: Invalid profile_key")
	end
	if DataEngine.SystemHalted then
		return nil
	end
	WaitStore(self)
	if ver~=nil and (um==MockTag or UseMock) then
		return nil
	end
	while not DataEngine.SystemHalted do
		local ld,ki=RunUpdate(
			self,pk,
			{
				ExistingProfileHandle=nil,
				MissingProfileHandle=function(data)
					data.Data=DeepCopy(self._profile_template)
					data.MetaData={
						ProfileCreateTime=os.time(),
						SessionLoadCount=0,
						ActiveSession=nil,
						ForceLoadSession=nil,
						MetaTags={},
					}
				end,
				EditProfile=nil,
			},
			um==MockTag,
			true,
			ver
		)
		MarkWriteCleanup(self._profile_store_lookup,pk)
		if ld then
			if not ki then
				return nil
			end
			local guo={
				_updates_latest=ld.GlobalUpdates,
				_record=nil,
			}
			setmetatable(guo,GUpdates)
			local rec={
				Data=ld.Data,
				MetaData=ld.MetaData,
				MetaTagsUpdated=Util.NewSignal(),
				RobloxMetaData=ld.RobloxMetaData or {},
				UserIds=ld.UserIds or {},
				KeyInfo=ki,
				KeyInfoUpdated=Util.NewSignal(),
				GlobalUpdates=guo,
				_record_store=self,
				_record_key=pk,
				_view_mode=true,
				_load_timestamp=os.clock(),
			}
			setmetatable(rec,Record)
			guo._record=rec
			return rec
		else
			task.wait()
		end
	end
	return nil
end

function RecordStorage:ProfileVersionQuery(pk,dir,min,max,um)
	if type(pk)~="string" or #pk==0 then
		error("[DataEngine]: Invalid profile_key")
	end
	if DataEngine.SystemHalted then
		return setmetatable({},VersionQuery)
	end
	WaitStore(self)
	if um==MockTag or UseMock then
		error("[DataEngine]: Not supported in mock mode")
	end
	if dir~=nil and (typeof(dir)~="EnumItem" or dir.EnumType~=Enum.SortDirection) then
		error("[DataEngine]: Invalid sort_direction")
	end
	if min~=nil and typeof(min)~="DateTime" and typeof(min)~="number" then
		error("[DataEngine]: Invalid min_date")
	end
	if max~=nil and typeof(max)~="DateTime" and typeof(max)~="number" then
		error("[DataEngine]: Invalid max_date")
	end
	min=typeof(min)=="DateTime" and min.UnixTimestampMillis or min
	max=typeof(max)=="DateTime" and max.UnixTimestampMillis or max
	local q={
		_profile_store=self,
		_profile_key=pk,
		_sort_dir=dir,
		_min=min,
		_max=max,
		_pages=nil,
		_idx=0,
		_fail=false,
		_yielded=false,
		_queue={},
	}
	setmetatable(q,VersionQuery)
	return q
end

function RecordStorage:WipeProfileAsync(pk,um)
	if type(pk)~="string" or #pk==0 then
		error("[DataEngine]: Invalid profile_key")
	end
	if DataEngine.SystemHalted then
		return false
	end
	WaitStore(self)
	local out=false
	if um==MockTag then
		local mk=MockUser[self._profile_store_lookup]
		if mk then
			mk[pk]=nil
		end
		out=true
		task.wait()
	elseif UseMock then
		local mm=MockMain[self._profile_store_lookup]
		if mm then
			mm[pk]=nil
		end
		out=true
		task.wait()
	else
		out=pcall(function()
			self._global_data_store:RemoveAsync(pk)
		end)
	end
	MarkWriteCleanup(self._profile_store_lookup,pk)
	return out
end

function DataEngine.GetProfileStore(idx,tmpl)
	local n
	local sc=nil
	if type(idx)=="string" then
		n=idx
	elseif type(idx)=="table" then
		n=idx.Name
		sc=idx.Scope
	else
		error("[DataEngine]: Invalid profile_store_index")
	end
	if not n or type(n)~="string" then
		error("[DataEngine]: Missing or invalid Name")
	elseif #n==0 then
		error("[DataEngine]: Empty ProfileStore name")
	end
	if sc~=nil and (type(sc)~="string" or #sc==0) then
		error("[DataEngine]: Invalid Scope")
	end
	if type(tmpl)~="table" then
		error("[DataEngine]: Invalid profile_template")
	end
	local ps
	ps={
		Mock={
			LoadProfileAsync=function(_,k,h)
				return ps:LoadProfileAsync(k,h,MockTag)
			end,
			GlobalUpdateProfileAsync=function(_,k,uh)
				return ps:GlobalUpdateProfileAsync(k,uh,MockTag)
			end,
			ViewProfileAsync=function(_,k,v)
				return ps:ViewProfileAsync(k,v,MockTag)
			end,
			FindProfileVersionAsync=function(_,k,d,mi,ma)
				return ps:FindProfileVersionAsync(k,d,mi,ma,MockTag)
			end,
			WipeProfileAsync=function(_,k)
				return ps:WipeProfileAsync(k,MockTag)
			end
		},
		_profile_store_name=n,
		_profile_store_scope=sc,
		_profile_store_lookup=n.."\0"..(sc or ""),
		_profile_template=tmpl,
		_global_data_store=nil,
		_loaded_profiles={},
		_profile_load_jobs={},
		_mock_loaded_profiles={},
		_mock_profile_load_jobs={},
		_is_pending=false,
	}
	setmetatable(ps,RecordStorage)
	local opt=Instance.new("DataStoreOptions")
	opt:SetExperimentalFeatures({v2=true})
	if LiveCheck then
		ps._is_pending=true
		task.spawn(function()
			WaitLive()
			if not UseMock then
				ps._global_data_store=DSService:GetDataStore(n,sc,opt)
			end
			ps._is_pending=false
		end)
	else
		if not UseMock then
			ps._global_data_store=DSService:GetDataStore(n,sc,opt)
		end
	end
	return ps
end

function DataEngine.IsLive()
	WaitLive()
	return not UseMock
end

if IsStudio then
	LiveCheck=true
	task.spawn(function()
		local s,m=pcall(function()
			DSService:GetDataStore("____PS"):SetAsync("____PS",os.time())
		end)
		local no_net=s==false and string.find(m,"ConnectFail",1,true)
		if no_net then
			warn("[DataEngine]: No internet")
		end
		if s==false and (string.find(m,"403",1,true) or string.find(m,"must publish",1,true) or no_net) then
			UseMock=true
			DataEngine._use_mock_data_store=true
			print("[DataEngine]: Roblox API unavailable")
		else
			print("[DataEngine]: Roblox API available")
		end
		LiveCheck=false
	end)
end

RunService.Heartbeat:Connect(function()
	local length=#AutoSaves
	if length>0 then
		local sp=CONFIG.AutoSaveCycle/length
		local now=os.clock()
		while now-LastSave>sp do
			LastSave+=sp
			local pf=AutoSaves[AutoIndex]
			if now-pf._load_timestamp<CONFIG.AutoSaveCycle then
				pf=nil
				for _=1,length-1 do
					AutoIndex+=1
					if AutoIndex>length then
						AutoIndex=1
					end
					pf=AutoSaves[AutoIndex]
					if now-pf._load_timestamp>=CONFIG.AutoSaveCycle then
						break
					else
						pf=nil
					end
				end
			end
			AutoIndex+=1
			if AutoIndex>length then
				AutoIndex=1
			end
			if pf then
				task.spawn(CommitSave,pf)
			end
		end
	end
	if not DataEngine.EmergencyMode then
		if #Issues>=CONFIG.IssuesForEmergency then
			DataEngine.EmergencyMode=true
			DataEngine.EmergencySignal:Fire(true)
			EmergencyStart=os.clock()
			warn("[DataEngine]: Entered critical state")
		end
	else
		if #Issues>=CONFIG.IssuesForEmergency then
			EmergencyStart=os.clock()
		elseif os.clock()-EmergencyStart>CONFIG.EmergencyDuration then
			DataEngine.EmergencyMode=false
			DataEngine.EmergencySignal:Fire(false)
			warn("[DataEngine]: Critical state ended")
		end
	end
	while true do
		local first=Issues[1]
		if not first then
			break
		elseif os.clock()-first>CONFIG.IssueExpiration then
			table.remove(Issues,1)
		else
			break
		end
	end
end)

task.spawn(function()
	WaitLive()
	Util.OnShutdown(function()
		DataEngine.SystemHalted=true
		local close_count=0
		local actives={}
		for i,p in ipairs(AutoSaves) do
			actives[i]=p
		end
		for _,pr in ipairs(actives) do
			if pr:IsActive() then
				close_count+=1
				task.spawn(function()
					CommitSave(pr,true)
					close_count-=1
				end)
			end
		end
		while close_count>0 or ActiveLoads>0 or ActiveSaves>0 do
			task.wait()
		end
		return
	end,not UseMock)
end)

return DataEngine