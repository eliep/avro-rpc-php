<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

require_once('test_helper.php');


class TestTransceiver extends SocketTransceiver {
	
	public $server = null;
	protected $response = null;
	
	public static function getTestClient($server)
	{
		$transceiver = new TestTransceiver();
		$transceiver->server = $server;
		
		return $transceiver;
	}
	
  public function read_message()
  {
		return $this->response;
  }
  
  /**
   * Writes a message into the channel. Blocks until the message has been written.
   * @param string $message
   */
  public function write_message($message)
  {
		$this->response = null;
		if (!is_null($this->server))
			$this->response = $this->server->process($message);
  }
  
  /**
   * Return the name of the socket remode side
   * @return string the remote name
   */
  public function remote_name()
  {
		return 'TestTransceiver';
  }
}

class TestServer {
	
	protected $responder;
	protected $transceiver;
	
	public function __construct(Responder $responder) {
		$this->responder = $responder;
		$this->transceiver = new TestTransceiver();
	}
	
	public function process($call_request) {
		$call_response = $this->responder->respond($call_request, $this->transceiver);
		if (!is_null($call_response))
			return $call_response;
	}
}

class TestProtocolResponder extends Responder {
  public function invoke( $local_message, $request) {
    switch ($local_message->name) {
      case "testSimpleRequestResponse":
        if ($request["message"]["subject"] == "ping")
          return array("response" => "pong");
        else if ($request["message"]["subject"] == "pong")
          return array("response" => "ping");
        break;
      case "testNotification":
        break;
      case "testRequestResponseException":
        throw new AvroRemoteException(array("exception" => "always"));
        break;
      default:
        throw new AvroRemoteException("Method unknown");
    }
  }
}


class IpcTest extends PHPUnit_Framework_TestCase
{
	
	public function testSimpleRequestResponse()
	{
		
		$server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
    $client = TestTransceiver::getTestClient($server);
    $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);
    
    $response = $requestor->request('testSimpleRequestResponse', array("message" => array("subject" => "ping")));
    $this->assertEquals("pong", $response["response"]);
    $response = $requestor->request('testSimpleRequestResponse', array("message" => array("subject" => "pong")));
    $this->assertEquals("ping", $response["response"]);
    
	}
	
	public function testNotification()
	{
		$server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
    $client = TestTransceiver::getTestClient($server);
    $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);
    
    $response = $requestor->request('testNotification', array("notification" => array("subject" => "notify")));
    $this->assertTrue($response);
    
	}
	
	public function testRequestResponseException()
	{
		$server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
    $client = TestTransceiver::getTestClient($server);
    $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);
    
    $exception_raised = false;
    try {
      $response = $requestor->request('testRequestResponseException', array("exception" => array("cause" => "test")));
    } catch (AvroRemoteException $e) {
      $exception_raised = true;
      $exception_datum = $e->getDatum();
      $this->assertEquals("always", $exception_datum["exception"]);
    }
    $this->assertTrue($exception_raised);
	}
	
	public function testMultipleCalls()
	{
		$server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
    $client = TestTransceiver::getTestClient($server);
    $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);
    
		$response = $requestor->request('testNotification', array("notification" => array("subject" => "notify")));
    $this->assertTrue($response);
		
    $response = $requestor->request('testSimpleRequestResponse', array("message" => array("subject" => "ping")));
    $this->assertEquals("pong", $response["response"]);
		
    $exception_raised = false;
    try {
      $response = $requestor->request('testRequestResponseException', array("exception" => array("cause" => "test")));
    } catch (AvroRemoteException $e) {
      $exception_raised = true;
      $exception_datum = $e->getDatum();
      $this->assertEquals("always", $exception_datum["exception"]);
    }
    $this->assertTrue($exception_raised);
	}
  
  private $protocol = <<<PROTO
{
 "namespace": "examples.protocol",
 "protocol": "TestProtocol",

 "types": [
     {"type": "record", "name": "SimpleRequest",
      "fields": [{"name": "subject",   "type": "string"}]
     },
     {"type": "record", "name": "SimpleResponse",
      "fields": [{"name": "response",   "type": "string"}]
     },
     {"type": "record", "name": "Notification",
      "fields": [{"name": "subject",   "type": "string"}]
     },
     {"type": "record", "name": "RaiseException",
      "fields": [{"name": "cause",   "type": "string"}]
     },
     {"type": "record", "name": "NeverSend",
      "fields": [{"name": "never",   "type": "string"}]
     },
     {"type": "record", "name": "AlwaysRaised",
      "fields": [{"name": "exception",   "type": "string"}]
     }
 ],

 "messages": {
     "testSimpleRequestResponse": {
         "doc" : "Simple Request Response",
         "request": [{"name": "message", "type": "SimpleRequest"}],
         "response": "SimpleResponse"
     },
     "testNotification": {
         "doc" : "Notification : one-way message",
         "request": [{"name": "notification", "type": "Notification"}],
         "one-way": true
     },
     "testRequestResponseException": {
         "doc" : "Request Response with Exception",
         "request": [{"name": "exception", "type": "RaiseException"}],
         "response" : "NeverSend",
         "errors" : ["AlwaysRaised"]
     }
 }
}
PROTO;

}