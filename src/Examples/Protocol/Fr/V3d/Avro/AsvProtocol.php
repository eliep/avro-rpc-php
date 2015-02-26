<?php

namespace Avro\Examples\Protocol\Fr\V3d\Avro;

use Avro\RPC\RpcProtocol;

class AsvProtocol extends RpcProtocol {
  
  private $jsonProtocol = <<<PROTO
{"protocol": "ASV",
 "namespace": "fr.v3d.avro",

 "types": [
     {"type": "record", "name": "Asv",
      "fields": [
          {"name": "a",   "type": "int"},
          {"name": "s", "type": "string"},
          {"name": "v", "type": "string"}
      ]
     },
     {"type": "record", "name": "Attention",
      "fields": [
          {"name": "status",   "type": "string"}
      ]
     }
     
 ],

 "messages": {
     "send": {
         "request": [{"name": "message", "type": "Asv"}],
         "response": "Attention"
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