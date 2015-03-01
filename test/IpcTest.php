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


class IpcTest extends PHPUnit_Framework_TestCase
{
  protected static $server_pid = null;
  protected $server_host = "127.0.0.1";
  protected $server_port = 1410;
  
	public static function setUpBeforeClass() {
    exec('php '.__DIR__.'/sample_rpc_server.php > /dev/null 2>&1 & echo $!', $pid);
    self::$server_pid = (int)$pid[0];
    sleep(1); // wait for the server to setup.
	}
	
	public function testSimpleRequestResponse() {
    $client = new SocketTransceiver($this->server_host, $this->server_port);
    $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);
    
    $response = $requestor->request('testSimpleRequestResponse', array("message" => array("subject" => "ping")));
    $this->assertEquals("pong", $response["response"]);
    $response = $requestor->request('testSimpleRequestResponse', array("message" => array("subject" => "pong")));
    $this->assertEquals("ping", $response["response"]);
    
    $client->close();
	}
	
	public function testNotification() {
    $client = new SocketTransceiver($this->server_host, $this->server_port);
    $requestor = new Requestor(AvroProtocol::parse($this->protocol), $client);
    
    $response = $requestor->request('testNotification', array("notification" => array("subject" => "notify")));
    $this->assertTrue($response);
    
    $client->close();
	}
	
	public function testRequestResponseException() {
    $client = new SocketTransceiver($this->server_host, $this->server_port);
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
    
    $client->close();
	}
  
  public static function  tearDownAfterClass() {
    exec("kill -9 ".self::$server_pid);
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