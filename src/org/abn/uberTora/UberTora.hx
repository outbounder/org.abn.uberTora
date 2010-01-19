package org.abn.uberTora;

import neko.net.Socket.SocketHandle;
import tora.Code;
import tora.Infos;

typedef ThreadData = {
	var id : Int;
	var t : neko.vm.Thread;
	var client : Client;
	var time : Float;
	var hits : Int;
	var notify : Int;
	var notifyRetry : Int;
	var errors : Int;
	var stopped : Bool;
}

typedef FileData = {
	var file : String;
	var filetime : Float;
	var loads : Int;
	var cacheHits : Int;
	var notify : Int;
	var bytes : Float;
	var time : Float;
	var lock : neko.vm.Mutex;
	var api : ModToraApi;
}

class UberTora 
{
	public static var inst : UberTora;
	static var STOP : Dynamic = {};
	static var MODIFIED : Dynamic = {};

	var clientQueue : neko.vm.Deque<Client>;
	var notifyQueue : neko.vm.Deque<Client>;
	var threads : Array<ThreadData>;
	var startTime : Float;
	var totalHits : Int;
	var recentHits : Int;
	var files : Hash<FileData>;
	var flock : neko.vm.Mutex;
	var rootLoader : neko.vm.Loader;
	var modulePath : Array<String>;
	var redirect : Dynamic;
	var set_trusted : Dynamic;
	var enable_jit : Bool -> Bool;
	var running : Bool;
	var jit : Bool;
	var hosts : Hash<String>;
	var ports : Array<Int>;
	
	public static function main() 
	{
		var host = "127.0.0.1";
		var port = 6666;
		var args = neko.Sys.args();
		var nthreads = 200;
		var i = 0;
		inst = new UberTora();
		while( true ) {
			var kind = args[i++];
			var value = function() { var v = args[i++]; if( v == null ) throw "Missing value for '"+kind+"'"; return v; };
			if( kind == null ) break;
			switch( kind ) {
			case "-h","-host": host = value();
			case "-p","-port": port = Std.parseInt(value());
			case "-t","-threads": nthreads = Std.parseInt(value());
			default: throw "Unknown argument "+kind;
			}
		}
		inst.init(nthreads);
		log("Starting UberTora server on "+host+":"+port+" with "+nthreads+" threads");
		inst.run(host,port,true);
		inst.stop();
	}
	
	public function new() 
	{
		totalHits = 0;
		recentHits = 0;
		running = true;
		startTime = haxe.Timer.stamp();
		files = new Hash();
		hosts = new Hash();
		ports = new Array();
		flock = new neko.vm.Mutex();
		clientQueue = new neko.vm.Deque();
		notifyQueue = new neko.vm.Deque();
		threads = new Array();
		rootLoader = neko.vm.Loader.local();
		modulePath = rootLoader.getPath();
	}
	
	private function init( nthreads : Int ) 
	{
		neko.Sys.putEnv("MOD_NEKO","1");
		redirect = neko.Lib.load("std","print_redirect",1);
		set_trusted = neko.Lib.load("std","set_trusted",1);
		enable_jit = neko.Lib.load("std","enable_jit",1);
		jit = (enable_jit(null) == true);
		neko.vm.Thread.create(callback(startup,nthreads));
		neko.vm.Thread.create(speedLoop);
	}
	
	private function run( host : String, port : Int, secure : Bool ) 
	{
		var s = new neko.net.Socket();
		try {
			s.bind(new neko.net.Host(host),port);
		} catch( e : Dynamic ) {
			throw "Failed to bind socket : invalid host or port is busy";
		}
		s.listen(400);
		ports.push(port);
		try {
			while( running ) {
				var sock = s.accept();
				totalHits++;
				clientQueue.add(new Client(sock,secure));
			}
		} catch( e : Dynamic ) {
			log("accept() failure : maybe too much FD opened ?");
		}
		// close our waiting socket
		s.close();
	}
	
	private function stop() 
	{
		log("Shuting down...");
		// inform all threads that we are stopping
		for( i in 0...threads.length )
			clientQueue.add(null);
		// our own marker
		clientQueue.add(null);
		var count = 0;
		while( true ) {
			var c = clientQueue.pop(false);
			if( c == null )
				break;
			c.sock.close();
			count++;
		}
		log(count + " sockets closed in queue...");
		// wait for threads to stop
		neko.Sys.sleep(5);
		count = 0;
		for( t in threads )
			if( t.stopped )
				count++;
			else
				log("Thread "+t.id+" is locked in "+((t.client == null)?"???":t.client.getURL()));
		log(count + " / " + threads.length + " threads stopped");
	}
	
	private function startup( nthreads : Int ) 
	{
		for( i in 0...nthreads ) {
			var inf : ThreadData = {
				id : i,
				t : null,
				client : null,
				hits : 0,
				notify : 0,
				notifyRetry : 0,
				errors : 0,
				time : haxe.Timer.stamp(),
				stopped : false,
			};
			inf.t = neko.vm.Thread.create(callback(threadLoop,inf));
			threads.push(inf);
		}
	}
	
	// measuring speed
	function speedLoop() 
	{
		while ( true ) 
		{
			var hits = totalHits, time = neko.Sys.time();
			neko.Sys.sleep(1.0);
			recentHits = Std.int((totalHits - hits) / (neko.Sys.time() - time));
		}
	}
	
	private function initLoader( api : ModToraApi ) 
	{
		var me = this;
		var mod_neko = neko.NativeString.ofString("mod_neko@");
		var mem_size = "std@mem_size";
		var self : neko.vm.Loader = null;
		var first_module = jit;
		var loadPrim = function(prim:String,nargs:Int) {
			if( untyped __dollar__sfind(prim.__s,0,mod_neko) == 0 ) {
				var p = Reflect.field(api,prim.substr(9));
				if( p == null || untyped __dollar__nargs(p) != nargs )
					throw "Primitive not found "+prim+" "+nargs;
				return untyped __dollar__varargs( function(args) return __dollar__call(p,api,args) );
			}
			if( prim == mem_size )
				return function(_) return 0;
			return me.rootLoader.loadPrimitive(prim,nargs);
		};
		var loadModule = function(module:String, l) 
		{
			var idx = module.lastIndexOf(".");
			if( idx >= 0 )
				module = module.substr(0,idx);
			var cache : Dynamic = untyped self.l.cache;
			var mod = Reflect.field(cache, module);
			if ( mod == null ) {
				if( first_module )
					me.enable_jit(true);
				mod = neko.vm.Module.readPath(module,me.modulePath,self);
				if( first_module ) {
					me.enable_jit(false);
					first_module = false;
				}
				Reflect.setField(cache,module,mod);
				mod.execute();
			}
			return mod;
		};
		self = neko.vm.Loader.make(loadPrim, loadModule);
		return self;
	}

	function threadLoop( t : ThreadData ) 
	{
		set_trusted(true);
		while ( true ) 
		{
			var client = clientQueue.pop(true);
			if( client == null ) {
				continue;
			}
			
			t.time = haxe.Timer.stamp();
			t.client = client;
			t.hits++;
			
			// retrieve request
			try 
			{
				client.sock.setTimeout(10);
				while ( !client.processMessage() ) {}
				if( client.execute && client.file == null )
					throw "Missing module file";
			} 
			catch ( e : Dynamic ) 
			{
				log("Error while reading request ("+Std.string(e)+")");
				t.errors++;
				client.execute = false;
			}
			
			// check if we need to do something
			if ( !client.execute ) 
			{
				log("nothing to do here. closing client sock");
				client.sock.close();
				t.client = null;
				continue;
			}
			
			var f = files.get(client.file);
			// file entry not found : we need to acquire
			// a global lock before setting the entry
			if ( f == null ) 
			{
				flock.acquire();
				f = files.get(client.file);
				if ( f == null ) 
				{
					f = {
						file : client.file,
						filetime : 0.,
						loads : 0,
						cacheHits : 0,
						notify : 0,
						api: new ModToraApi(),
						lock : new neko.vm.Mutex(),
						bytes : 0.,
						time : 0.,
					};
					files.set(client.file,f);
				}
				flock.release();
			}
			
			// check if up-to-date cache is available
			f.lock.acquire();
			
			f.api.client = client;
			redirect(f.api.print);
			
			var time = getFileTime(client.file);
			if ( time != f.filetime || f.api.main == null) 
			{
				f.loads++;
				f.filetime = time;
				initLoader(f.api).loadModule(client.file);
			}
			else
				f.cacheHits++;
			
			// execute
			var code = CExecute;
			var data = "";
			try 
			{
				if(f.api.main != null)
					f.api.main();
			}
			catch ( e : Dynamic ) 
			{
				code = CError;
				data = try Std.string(e) + haxe.Stack.toString(haxe.Stack.exceptionStack()) catch( _ : Dynamic ) "??? TORA Error";
			}
			
			f.lock.release();
			
			// send result
			try 
			{
				client.sendHeaders(); // if no data has been printed
				client.sock.setFastSend(true);
				client.sendMessage(code,data);
			} catch( e : Dynamic ) {
				if( client.secure ) log("Error while sending answer ("+Std.string(e)+")");
				t.errors++;
			}
			
			// save infos
			f.lock.acquire();
			f.time += haxe.Timer.stamp() - t.time;
			f.bytes += client.dataBytes;
			f.api.client = null;
			f.lock.release();
			
			// cleanup
			redirect(null);
			t.client = null;
			client.sock.close();
		}
	}
	
	private function getFileTime( file ) 
	{
		return try neko.FileSystem.stat(file).mtime.getTime() catch( e : Dynamic ) 0.;
	}

	public function command( cmd : String, param : String ) : Void 
	{
		switch( cmd ) {
		case "stop":
			running = false;
		case "gc":
			neko.vm.Gc.run(true);
		case "clean":
			flock.acquire();
			for( f in files.keys() )
				files.remove(f);
			flock.release();
		default:
			throw "No such command '"+cmd+"'";
		}
	}

	public function infos() : Infos 
	{
		var tinf = new Array();
		var tot = 0;
		var notify = 0, notifyRetry = 0;
		for( t in threads ) {
			var cur = t.client;
			var ti : ThreadInfos = {
				hits : t.hits,
				errors : t.errors,
				file : (cur == null) ? null : (cur.file == null ? "???" : cur.file),
				url : (cur == null) ? null : cur.getURL(),
				time : (haxe.Timer.stamp() - t.time),
			};
			tot += t.hits;
			notify += t.notify;
			notifyRetry += t.notifyRetry;
			tinf.push(ti);
		}
		var finf = new Array();
		for( f in files ) {
			var f : FileInfos = {
				file : f.file,
				loads : f.loads,
				cacheHits : f.cacheHits,
				cacheCount : f.api != null?1:0,
				bytes : f.bytes,
				time : f.time,
			};
			finf.push(f);
		}
		return {
			threads : tinf,
			files : finf,
			totalHits : totalHits,
			recentHits : recentHits,
			notify : notify,
			notifyRetry : notifyRetry,
			queue : totalHits - tot,
			upTime : haxe.Timer.stamp() - startTime,
			jit : jit,
		};
	}
	
	public function resolveHost( name : String ) 
	{
		return hosts.get(name);
	}

	public static function log( msg : String ) 
	{
		neko.io.File.stderr().writeString("["+Date.now().toString()+"] "+msg+"\n");
	}
}