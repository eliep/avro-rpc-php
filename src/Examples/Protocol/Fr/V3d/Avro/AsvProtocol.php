<?php

namespace Avro\Examples\Protocol\Fr\V3d\Avro;

use Avro\Protocol\Protocol;

class AsvProtocol extends Protocol {
  
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
    return $this->abstractRequest(array($message));
  }
  
  public function sendImpl($callback) {
    $this->abstractResponse($callback);
  }
  
}