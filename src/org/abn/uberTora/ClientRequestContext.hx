/**
 * ...
 * @author outbounder
 */

package org.abn.uberTora;

import neko.NativeString;
import tora.Code;

class ClientRequestContext
{
	private var client:Client;
	
	public function new(client:Client) 
	{
		this.client = client;
	}
	
	public function get_host_name():String {
		return client.hostName;
	}

	public function get_client_ip():String {
		return client.ip;
	}

	public function get_uri():String {
		return client.uri;
	}

	public function redirect( url : String ):Void {
		addHeader("Redirection",CRedirect,url);
	}

	public function set_return_code( code : Int ):Void {
		addHeader("Return code",CReturnCode,Std.string(code));
	}

	public function get_client_header( header : String ):String {
		var c;
		var hl = header.toLowerCase();
		for( h in client.headers )
			if( h.k.toLowerCase() == hl )
				return h.v;
		return null;
	}

	public function get_params_string():String {
		var p = client.getParams;
		if( p == null ) return null;
		return p;
	}

	public function get_post_data():String {
		var p = client.postData;
		if( p == null ) return null;
		return p;
	}

	public function get_params():String {
		return makeTable(client.params);
	}

	public function cgi_get_cwd():String {
		var path = client.file.split("/");
		if( path.length > 0 )
			path.pop();
		return path.join("/")+"/";
	}

	public function get_http_method():String {
		return client.httpMethod;
	}

	public function set_header( header : String, value : String ):Void {
		var h = header;
		addHeader(h,CHeaderKey,header);
		addHeader(h,CHeaderValue,value);
	}

	public function get_cookies():Dynamic {
		var v : Dynamic = null;
		var c = get_client_header("Cookie");
		if( c == null ) return v;
		var start = 0;
		var tmp = neko.Lib.bytesReference(c);
		while( true ) {
			var begin = c.indexOf("=",start);
			if( begin < 0 ) break;
			var end = begin + 1;
			while( true ) {
				var c = tmp.get(end);
				if( c == null || c == 10 || c == 13 || c == 59 )
					break;
				end++;
			}
			v = untyped __dollar__array(
				NativeString.ofString(c.substr(start,begin-start)),
				NativeString.ofString(c.substr(begin+1,end-begin-1)),
				v
			);
			if( tmp.get(end) != 59 || tmp.get(end+1) != 32 )
				break;
			start = end + 2;
		}
		return v;
	}

	public function set_cookie( name : String, value : String ):Void {
		var buf = new StringBuf();
		buf.add(name);
		buf.add("=");
		buf.add(value);
		buf.add(";");
		addHeader("Cookie",CHeaderKey,"Set-Cookie");
		addHeader("Cookie",CHeaderAddValue,buf.toString());
	}

	public function parse_multipart_data( onPart : NativeString -> NativeString -> Void, onData : NativeString -> Int -> Int -> Void ) {
		var bufsize = 1 << 16;
		client.sendMessage(CQueryMultipart,Std.string(bufsize));
		var filename = null;
		var buffer = haxe.io.Bytes.alloc(bufsize);
		var error = null;
		while( true ) {
			var msg = client.readMessageBuffer(buffer);
			switch( msg ) {
			case CExecute:
				break;
			case CPartFilename:
				filename = buffer.sub(0,client.bytes).getData();
			case CPartKey:
				if( error == null )
					try {
						onPart( buffer.sub(0,client.bytes).getData(), filename );
					} catch( e : Dynamic ) {
						error = { r : e };
					}
				filename = null;
			case CPartData:
				if( error == null )
					try {
						onData( buffer.getData(), 0, client.bytes );
					} catch( e : Dynamic ) {
						error = { r : e };
					}
			case CPartDone:
			case CError:
				throw buffer.readString(0,client.bytes);
			default:
				throw "Unexpected "+msg;
			}
		}
		if( error != null )
			neko.Lib.rethrow(error.r);
	}

	public function flush():Void {
		client.sendHeaders();
		client.sendMessage(CFlush,"");
	}

	public function get_client_headers():Dynamic {
		return makeTable(client.headers);
	}

	public function log_message( msg : String ):Dynamic {
		client.sendMessage(CLog,msg);
	}

	// internal APIS

	public function sendResponse( value : Dynamic ) {
		var str = NativeString.toString(untyped if( __dollar__typeof(value) == __dollar__tstring ) value else __dollar__string(value));
		client.sendHeaders();
		client.dataBytes += str.length;
		client.sendMessage(CPrint,str);
	}

	function addHeader( msg : String, c : Code, str : String ) {
		if( client.headersSent ) throw NativeString.ofString("Cannot set "+msg+" : Headers already sent");
		client.outputHeaders.add({ code : c, str : str });
	}

	static function makeTable( list : Array<{ k : String, v : String }> ) : Dynamic {
		var v : Dynamic = null;
		for( h in list )
			v = untyped __dollar__array(NativeString.ofString(h.k),NativeString.ofString(h.v),v);
		return v;
	}

	
}