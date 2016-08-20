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
require_once __DIR__."/../lib/avro.php";

$protocol = <<<PROTO
{
 "namespace": "protocol",
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
     "testNotNamedResponse": {
         "doc" : "Simple Request Response",
         "request": [{"name": "message", "type": "SimpleRequest"}],
         "response": {"type": "map", "values": "string"}
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

$client = NettyFramedSocketTransceiver::create('127.0.0.1', 1411);
$requestor = new Requestor(AvroProtocol::parse($protocol), $client);

try {
  $response = $requestor->request('testSimpleRequestResponse', array("message" => array("subject" => "pong")));
  echo "Response received: ".json_encode($response)."\n";
  $response = $requestor->request('testSimpleRequestResponse', array("message" => array("subject" => "ping")));
  echo "Response received: ".json_encode($response)."\n";
} catch (AvroRemoteException $e) {
  echo "Exception received: ".json_encode($e->getDatum()).", ".$e->getMessage()."\n";
}

//try {
  $response = $requestor->request('testNotNamedResponse', array("message" => array("subject" => "pong")));
  echo "Response received: ".json_encode($response)."\n";
//} catch (AvroRemoteException $e) {
  //echo "xxx";
  //echo "Exception received: ".json_encode($e->getDatum()).", ".$e->getMessage()."\n";
//}

try {
  $response = $requestor->request('testNotification', array("notification" => array("subject" => "notify")));
  echo "Response received: ".json_encode($response)."\n";
} catch (AvroRemoteException $e) {
  echo "Exception received: ".json_encode($e->getDatum())."\n";
}

try {
  $response = $requestor->request('testRequestResponseException', array("exception" => array("cause" => "callback")));
  echo "Response received: ".json_encode($response)."\n";
} catch (AvroRemoteException $e) {
  $exception_datum = $e->getDatum();
  echo "Exception received: ".json_encode($exception_datum)."\n";
}


try {
  $response = $requestor->request('testRequestResponseException', array("exception" => array("cause" => "system")));
  echo "Response received: ".json_encode($response)."\n";
} catch (AvroRemoteException $e) {
  $exception_datum = $e->getDatum();
  echo "Exception received: ".json_encode($exception_datum)."\n";
}
$client->close();

