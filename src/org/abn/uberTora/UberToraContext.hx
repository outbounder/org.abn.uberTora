package org.abn.uberTora;

class UberToraContext extends ModToraApi
{
	public var requestHandler:Dynamic->Void;
	
	public function new() 
	{
		super();
	}
	
	public function redirectRequests(handler:Dynamic->Void):Void
	{
		this.requestHandler = handler;
	}
	
}