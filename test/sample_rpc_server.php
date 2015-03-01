<?php



require_once __DIR__."/../lib/avro.php";

$protocol = <<<PROTO
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

class TestProtocolResponder extends Responder {
  public function invoke( $local_message, $request) {
    echo $local_message->name.":".json_encode($request)."\n";
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

$server = new SocketServer('127.0.0.1', 1410, new TestProtocolResponder(AvroProtocol::parse($protocol)));
$server->start();


/*
require_once __DIR__."/../../vendor/autoload.php";

use Avro\Examples\Protocol\Fr\V3d\Avro\ASVProtocol;


$protocol = ASVProtocol::getServer('127.0.0.1', 1424);
$protocol->sendImpl(function($params) {
  $msg = $params[0];
  return array("status"=>"toto");
});
*/