/* ************************************************************************ */
/*																			*/
/*  Tora - Neko Application Server											*/
/*  Copyright (c)2008 Motion-Twin											*/
/*																			*/
/* This library is free software; you can redistribute it and/or			*/
/* modify it under the terms of the GNU Lesser General Public				*/
/* License as published by the Free Software Foundation; either				*/
/* version 2.1 of the License, or (at your option) any later version.		*/
/*																			*/
/* This library is distributed in the hope that it will be useful,			*/
/* but WITHOUT ANY WARRANTY; without even the implied warranty of			*/
/* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU		*/
/* Lesser General Public License or the LICENSE file for more details.		*/
/*																			*/
/* ************************************************************************ */
import neko.net.Socket;
import tora.Code;
import org.abn.uberTora.UberTora;

class Client {

	static var CODES = Type.getEnumConstructs(Code);

	// protocol
	public var sock : neko.net.Socket;
	public var data : String;
	public var bytes : Int;
	public var dataBytes : Int;

	// variables
	public var execute : Bool;
	public var file : String;
	public var uri : String;
	public var ip : String;
	public var getParams : String;
	public var postData : String;
	public var headers : Array<{ k : String, v : String }>;
	public var params : Array<{ k : String, v : String }>;
	public var hostName : String;
	public var httpMethod : String;
	public var headersSent : Bool;
	public var outputHeaders : List<{ code : Code, str : String }>;

	// queue variables
	public var secure : Bool;
	var key : String;

	public function new(s:Socket, secure:Bool)
	{
		sock = s;
		this.secure = secure;
		dataBytes = 0;
		headersSent = false;
		headers = new Array();
		outputHeaders = new List();
		params = new Array();
	}

	public function sendHeaders() {
		if( headersSent ) return;
		headersSent = true;
		for( h in outputHeaders )
			sendMessage(h.code,h.str);
	}

	public function readMessageBuffer( buf : haxe.io.Bytes ) : Code {
		var i = sock.input;
		var code = i.readByte();
		if( code == 0 || code > CODES.length )
			throw "Invalid proto code "+code;
		bytes = i.readUInt24();
		i.readFullBytes(buf,0,bytes);
		return Reflect.field(Code,CODES[code-1]);
	}

	public function readMessage() : Code {
		var i = sock.input;
		var code = i.readByte();
		if( code == 0 || code > CODES.length ) {
			if( code == "<".code ) {
				try {
					data = i.readString(22);
				} catch( e : Dynamic ) {
					data = null;
				}
			}
			throw "Invalid proto code "+code;
		}
		var len = i.readUInt24();
		data = i.readString(len);
		return Reflect.field(Code,CODES[code-1]);
	}

	public function sendMessage( code : Code, msg : String ) {
		var o = sock.output;
		o.writeByte( Type.enumIndex(code) + 1 );
		o.writeUInt24( msg.length );
		o.writeString( msg );
	}

	public function processMessage() {
		var code = readMessage();
		//trace(Std.string(code)+" ["+data+"]");
		switch( code ) {
		case CFile: if( secure ) file = data;
		case CUri: uri = data;
		case CClientIP: if( secure ) ip = data;
		case CGetParams: getParams = data;
		case CPostData: postData = data;
		case CHeaderKey: key = data;
		case CHeaderValue, CHeaderAddValue: headers.push({ k : key, v : data });
		case CParamKey: key = data;
		case CParamValue: params.push({ k : key, v : data });
		case CHostName: if( secure ) hostName = data;
		case CHttpMethod: httpMethod = data;
		case CExecute: execute = true; return true;
		case CTestConnect: execute = false; return true;
		case CHostResolve:
			hostName = data;
			ip = sock.peer().host.toString();
			file = UberTora.inst.resolveHost(hostName);
			httpMethod = "TORA";
			if( file == null ) {
				sendMessage(CError,"Unknown host");
				execute = false;
				return true;
			}
		case CError: throw data;
		default: throw "Unexpected "+Std.string(code);
		}
		data = null;
		return false;
	}

	public function getURL() {
		var h = hostName;
		var u = uri;
		if( h == null ) h = "???";
		if( u == null ) u = "/???";
		return h + u;
	}

}
