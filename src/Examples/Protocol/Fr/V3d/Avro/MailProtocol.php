<?php

namespace Avro\Examples\Protocol\Fr\V3d\Avro;

use Avro\RPC\RpcProtocol;

class MailProtocol extends RpcProtocol {
  
  private $jsonProtocol = <<<PROTO
{"namespace": "protocol",
 "protocol": "Mail",

 "types": [
     {"name": "Message", "type": "record",
      "fields": [
          {"name": "to",   "type": "string"},
          {"name": "from", "type": "string"},
          {"name": "body", "type": "string"}
      ]
     },
     {"name": "Reply", "type": "record",
      "fields": [
          {"name": "body",   "type": "string"}
      ]
     }
 ],

 "messages": {
     "send": {
         "request": [{"name": "message", "type": "Message"}],
         "response": "Reply"
     }
 }
}
PROTO;

  public function getJsonProtocol() {
    return $this->jsonProtocol;
  }
  
  public function send($message) {
    return $this->genericRequest(array($message));
  }
  
  public function sendImpl($callback) {
    $this->genericResponse($callback);
  }
  
}